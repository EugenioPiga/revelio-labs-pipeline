#!/usr/bin/env python3
"""Founder-quality V3: U.S.-year, U.S.-educated compatibility analysis.

This is a deliberate compatibility re-estimation using the restrictions from
ppml_tenure_ladder_main_revised_nocohorts.R:
  * annual source panel: 2000--2024;
  * U.S. individual-year: coalesce(last_country, first_country) == United States;
  * U.S. educated: last_university_country == United States;
  * outcome windows: 2000--2019 and 2010--2019.

The founder event remains first observed founder-spell entry, not legal birth.
Since the firm-year panel has no validated firm-country variable, the U.S.
restriction is imposed exactly at the inventor-year/founder-entry and history
level. Firm outcomes are the validated firm patent/citation measures and are
restricted by calendar window, not asserted to be U.S.-firm-only outcomes.
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

PROJECT = Path('/home/epiga/revelio_labs')
V3_FINAL = PROJECT / 'output/founder_firm_outcomes_v3/founder_firm_outcomes_v3'
V3_FIRM_YEAR = PROJECT / 'output/founder_firm_outcomes_v3/firm_year_outcomes_current_rcid_v3'
V3_CORE = Path('/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged')
PRIMARY_IMMIGRATION = 'immig_first_deg_or_job_nonUS'
US = 'United States'
INPUT_START, INPUT_END = 2000, 2024
WINDOWS = {'2000_2019': (2000, 2019), '2010_2019': (2010, 2019)}
USER_FIRM_KEY = ['user_id', 'founded_rcid']


def require_columns(df: DataFrame, cols: Iterable[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f'{label} missing required columns: {missing}')


def nonempty(c: str) -> F.Column:
    x = F.trim(F.col(c).cast('string'))
    return F.when((x.isNotNull()) & (x != ''), x)


def make_link_id(df: DataFrame) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast('string'), F.lit('__NULL__')) for c in [
        'user_id', 'founded_rcid', 'founder_position_id', 'first_founder_entry_year'
    ]]
    return df.withColumn('analysis_link_id', F.sha2(F.concat_ws('||', *parts), 256))


def pvals(z: np.ndarray) -> np.ndarray:
    return np.array([math.erfc(abs(float(v)) / math.sqrt(2.0)) if np.isfinite(v) else np.nan for v in z])


def collapse_rare(s: pd.Series, min_n: int = 60, max_levels: int = 80) -> pd.Series:
    x = s.astype('string').fillna('__MISSING__')
    counts = x.value_counts(dropna=False)
    keep = counts[counts >= min_n].index.tolist()[:max_levels]
    return x.where(x.isin(keep), '__OTHER__')


def design_matrix(df: pd.DataFrame, numeric: list[str], binary: list[str], categorical: list[str]):
    pieces = [pd.DataFrame({'const': np.ones(len(df))}, index=df.index)]
    used = []
    for c in numeric:
        if c not in df:
            continue
        x = pd.to_numeric(df[c], errors='coerce')
        if x.notna().sum() == 0:
            continue
        sd = float(x.std(ddof=0))
        if not np.isfinite(sd) or sd <= 0:
            continue
        med = float(x.median())
        pieces.append(pd.DataFrame({f'z_{c}': (x.fillna(med) - med) / sd}, index=df.index))
        if x.isna().any():
            pieces.append(pd.DataFrame({f'missing_{c}': x.isna().astype(float)}, index=df.index))
        used.append(c)
    for c in binary:
        if c not in df:
            continue
        x = pd.to_numeric(df[c], errors='coerce')
        if x.notna().sum() == 0 or x.nunique(dropna=True) <= 1:
            continue
        pieces.append(pd.DataFrame({c: x.fillna(0.0).astype(float)}, index=df.index))
        if x.isna().any():
            pieces.append(pd.DataFrame({f'missing_{c}': x.isna().astype(float)}, index=df.index))
        used.append(c)
    for c in categorical:
        if c not in df:
            continue
        d = pd.get_dummies(collapse_rare(df[c]), prefix=c, dtype=float)
        if d.shape[1] > 1:
            d = d.iloc[:, 1:]
        if d.shape[1]:
            pieces.append(d)
            used.append(c)
    X = pd.concat(pieces, axis=1)
    X = X.loc[:, X.nunique(dropna=False) > 1]
    if 'const' not in X:
        X.insert(0, 'const', 1.0)
    return X.astype(float), used


def valid_arrays(y: pd.Series, X: pd.DataFrame, w: pd.Series, clusters: pd.Series):
    ok = y.notna() & np.isfinite(y) & w.notna() & np.isfinite(w) & (w > 0) & clusters.notna()
    Xv = X.loc[ok].copy()
    keep = Xv.columns[(Xv.abs().sum(axis=0) > 0).to_numpy()]
    Xv = Xv.loc[:, keep]
    return (y.loc[ok].to_numpy(float), Xv.to_numpy(float), w.loc[ok].to_numpy(float),
            clusters.loc[ok].astype(str).to_numpy(), list(Xv.columns))


def cluster_cov(x: np.ndarray, bread: np.ndarray, scores: np.ndarray, clusters: np.ndarray):
    n, p = x.shape
    unique = np.unique(clusters)
    meat = np.zeros((p, p))
    for g in unique:
        s = scores[clusters == g].sum(axis=0)
        meat += np.outer(s, s)
    G = len(unique)
    correction = (G / max(G - 1, 1)) * ((n - 1) / max(n - p, 1)) if G > 1 else 1.0
    return correction * (bread @ meat @ bread)


def fit_wls(y, X, w, clusters, sample, model):
    yv, x, ww, cl, terms = valid_arrays(y, X, w, clusters)
    n, p = x.shape; G = len(np.unique(cl))
    if n <= p or G < 2:
        raise RuntimeError(f'Insufficient WLS support: n={n}, p={p}, clusters={G}')
    xtwx = x.T @ (ww[:, None] * x)
    bread = np.linalg.pinv(xtwx, rcond=1e-11)
    beta = bread @ (x.T @ (ww * yv))
    resid = yv - x @ beta
    cov = cluster_cov(x, bread, x * (ww * resid)[:, None], cl)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    z = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    tab = pd.DataFrame({'estimator':'WLS_log1p','sample':sample,'model':model,'term':terms,
                        'estimate':beta,'cluster_se':se,'z_approx':z,
                        'p_value_normal_approx':pvals(z),'n_observations':n,'n_clusters':G})
    diag = {'estimator':'WLS_log1p','sample':sample,'model':model,'n_observations':n,
            'n_clusters':G,'n_parameters':p,'matrix_rank':int(np.linalg.matrix_rank(x)),
            'condition_number':float(np.linalg.cond(xtwx)),'converged':True,'iterations':1}
    return tab, diag


def fit_ppml(y, X, w, clusters, sample, model, max_iter=150, tol=1e-9):
    yv, x, ww, cl, terms = valid_arrays(y, X, w, clusters)
    n, p = x.shape; G = len(np.unique(cl))
    if np.any(yv < 0) or n <= p or G < 2:
        raise RuntimeError(f'Invalid PPML support: n={n}, p={p}, clusters={G}')
    beta = np.zeros(p); converged = False
    for it in range(1, max_iter + 1):
        eta = np.clip(x @ beta, -20, 20); mu = np.exp(eta)
        xtwx = x.T @ ((ww * mu)[:, None] * x)
        bread = np.linalg.pinv(xtwx, rcond=1e-11)
        step = bread @ (x.T @ (ww * (yv - mu)))
        old = float(np.sum(ww * (yv * eta - mu))); scale = 1.0; accepted = False
        for _ in range(20):
            candidate = beta + scale * step
            eta_c = np.clip(x @ candidate, -20, 20); mu_c = np.exp(eta_c)
            obj = float(np.sum(ww * (yv * eta_c - mu_c)))
            if np.isfinite(obj) and obj >= old - 1e-9:
                beta = candidate; accepted = True; break
            scale *= 0.5
        if not accepted:
            break
        if np.max(np.abs(scale * step)) < tol:
            converged = True; break
    eta = np.clip(x @ beta, -20, 20); mu = np.exp(eta)
    xtwx = x.T @ ((ww * mu)[:, None] * x)
    bread = np.linalg.pinv(xtwx, rcond=1e-11)
    cov = cluster_cov(x, bread, x * (ww * (yv - mu))[:, None], cl)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    z = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    tab = pd.DataFrame({'estimator':'PPML','sample':sample,'model':model,'term':terms,
                        'estimate':beta,'cluster_se':se,'z_approx':z,
                        'p_value_normal_approx':pvals(z),'n_observations':n,'n_clusters':G})
    diag = {'estimator':'PPML','sample':sample,'model':model,'n_observations':n,
            'n_clusters':G,'n_parameters':p,'matrix_rank':int(np.linalg.matrix_rank(x)),
            'condition_number':float(np.linalg.cond(xtwx)),'converged':bool(converged), 'iterations':it}
    return tab, diag


def plot_coefficients(reg: pd.DataFrame, out_dir: Path, window: str, estimator: str):
    wanted = ['founder_stem_at_founder','z_us_log1p_prior_patents','z_us_log1p_prior_citations',
              'z_us_log1p_prior_distinct_rcids','z_founder_seniority']
    x = reg[(reg['window'] == window) & (reg['estimator'] == estimator) & reg['term'].isin(wanted)].copy()
    if x.empty:
        return
    x = x[x['sample'].isin(['B_us_edu_history_location_role','C_us_edu_prior_patents_only','D_us_edu_prior_citations_only'])]
    if x.empty:
        return
    x['low'] = x['estimate'] - 1.96*x['cluster_se']; x['high'] = x['estimate'] + 1.96*x['cluster_se']
    labels = {'founder_stem_at_founder':'STEM at entry','z_us_log1p_prior_patents':'Prior patents (1 SD)',
              'z_us_log1p_prior_citations':'Prior citations (1 SD)','z_us_log1p_prior_distinct_rcids':'Prior firms (1 SD)',
              'z_founder_seniority':'Seniority (1 SD)'}
    x['label'] = x['term'].map(labels)
    x = x.sort_values(['model','label'])
    fig, ax = plt.subplots(figsize=(9, 5.2))
    y = np.arange(len(x))
    ax.errorbar(x['estimate'], y, xerr=[x['estimate']-x['low'], x['high']-x['estimate']], fmt='o')
    ax.axvline(0, color='black', lw=.8); ax.set_yticks(y, x['label'] + ' | ' + x['sample'])
    ax.set_xlabel('Coefficient'); ax.set_title(f'{window}: U.S.-year, U.S.-educated founder associations ({estimator})')
    fig.tight_layout(); fig.savefig(out_dir / f'{window}_coefficients_{estimator}.png', dpi=220); fig.savefig(out_dir / f'{window}_coefficients_{estimator}.pdf'); plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--validation-run', required=True)
    ap.add_argument('--out-root', default=str(PROJECT/'output/founder_quality_v1/phase3_us_edu_v3'))
    ap.add_argument('--shuffle-partitions', type=int, default=320)
    args = ap.parse_args()
    validation = Path(args.validation_run).resolve()
    elig_path = validation/'candidate_link_horizon_eligibility'
    if not (validation/'VALIDATION_REPORT.md').exists() or not elig_path.exists():
        raise RuntimeError('A completed V3 validation run with candidate_link_horizon_eligibility is required.')
    run = Path(args.out_root)/datetime.now(timezone.utc).strftime('run_%Y%m%dT%H%M%SZ')
    run.mkdir(parents=True, exist_ok=False); tables = run/'tables'; tables.mkdir(); figs = run/'figures'; figs.mkdir(); panels = run/'panels'; panels.mkdir()
    spark = (SparkSession.builder.appName('founder_quality_phase3_us_edu_v3')
             .config('spark.sql.adaptive.enabled','true')
             .config('spark.sql.shuffle.partitions', str(args.shuffle_partitions))
             .config('spark.sql.execution.arrow.pyspark.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    print(f'[INFO] run_dir={run}', flush=True)

    final = spark.read.parquet(str(V3_FINAL)); fy = spark.read.parquet(str(V3_FIRM_YEAR)); core = spark.read.parquet(str(V3_CORE)); elig = spark.read.parquet(str(elig_path))
    require_columns(final, ['user_id','founded_rcid','founder_position_id','first_founder_entry_date','first_founder_entry_year','core_founder_title_flag',PRIMARY_IMMIGRATION,'firm_patents_y1_5','firm_us_citations_y1_5','founder_stem_at_founder','founder_seniority'], 'V3 founder output')
    require_columns(fy, ['firm_rcid','year','firm_patents','firm_us_citations'], 'V3 firm-year output')
    require_columns(core, ['user_id','year','first_country','last_country','last_university_country','n_patents','n_us_citations','current_rcid'], 'V3 inventor-year core')
    require_columns(elig, ['analysis_link_id','eligible_patents_y0_5','eligible_citations_y0_5'], 'validated eligibility')

    optional = [c for c in ['founder_naics3','founder_metro_area','founder_mapped_role_v3'] if c in final.columns]
    keep = ['user_id','founded_rcid','founder_position_id','first_founder_entry_date','first_founder_entry_year','core_founder_title_flag',PRIMARY_IMMIGRATION,'firm_patents_y1_5','firm_us_citations_y1_5','founder_stem_at_founder','founder_seniority'] + optional
    order = Window.partitionBy(*USER_FIRM_KEY).orderBy(F.col('first_founder_entry_date').asc_nulls_last(), F.col('founder_position_id').asc_nulls_last())
    links = make_link_id(final.select(*keep).withColumn('_rn',F.row_number().over(order)).filter(F.col('_rn')==1).drop('_rn'))
    links = links.persist(StorageLevel.MEMORY_AND_DISK)
    n_dedup = links.count()
    base = (links.join(elig.select('analysis_link_id','eligible_patents_y0_5','eligible_citations_y0_5').dropDuplicates(['analysis_link_id']), 'analysis_link_id')
            .filter(F.coalesce(F.col('core_founder_title_flag').cast('int'),F.lit(0))==1)
            .filter((F.col('eligible_patents_y0_5')==1)&(F.col('eligible_citations_y0_5')==1)))

    # Exact legacy compatibility restriction: U.S. individual-year by last_country then first_country fallback, plus U.S. last university.
    us_panel = (
        core.select(
            'user_id',
            F.col('year').cast('int').alias('panel_year'),
            F.col('n_patents').cast('double').alias('panel_patents'),
            F.col('n_us_citations').cast('double').alias('panel_citations'),
            F.col('current_rcid').cast('string').alias('panel_rcid'),
            nonempty('last_country').alias('_last_country'),
            nonempty('first_country').alias('_first_country'),
            nonempty('last_university_country').alias('_last_university_country'),
        )
        .filter((F.col('panel_year') >= F.lit(INPUT_START)) & (F.col('panel_year') <= F.lit(INPUT_END)))
    )
    us_panel = (
        us_panel.withColumn('country_year', F.coalesce(F.col('_last_country'), F.col('_first_country')))
        .filter((F.col('country_year') == US) & (F.col('_last_university_country') == US))
        .drop('_last_country', '_first_country', '_last_university_country')
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    raw_us_panel_n = us_panel.count()
    us_users = us_panel.select('user_id').distinct()
    entry_us = us_panel.select('user_id',F.col('panel_year').alias('first_founder_entry_year')).distinct()
    eligible_entry = base.join(entry_us, ['user_id','first_founder_entry_year'], 'inner').persist(StorageLevel.MEMORY_AND_DISK)
    n_core_eligible = base.count(); n_us_edu_entry = eligible_entry.count()

    # Recompute histories only from the same U.S.-year/U.S.-educated raw panel, using strictly pre-entry calendar years.
    candidate_users = eligible_entry.select('user_id').distinct()
    hist_panel = us_panel.join(candidate_users, 'user_id', 'inner')
    hist_keys = eligible_entry.select('analysis_link_id','user_id','first_founder_entry_year')
    hist = (hist_keys.join(hist_panel, 'user_id', 'left')
            .filter(F.col('panel_year') < F.col('first_founder_entry_year'))
            .groupBy('analysis_link_id').agg(
                F.countDistinct('panel_year').alias('us_prior_years_observed'),
                F.sum(F.coalesce(F.col('panel_patents'),F.lit(0.0))).alias('us_prior_patents'),
                F.sum(F.coalesce(F.col('panel_citations'),F.lit(0.0))).alias('us_prior_us_citations'),
                F.countDistinct(F.when(F.col('panel_rcid').isNotNull()&(F.col('panel_rcid')!=''),F.col('panel_rcid'))).alias('us_prior_distinct_rcids')))
    candidate = (eligible_entry.join(hist,'analysis_link_id','left')
                 .withColumn('us_prior_years_observed',F.coalesce(F.col('us_prior_years_observed'),F.lit(0)).cast('double'))
                 .withColumn('us_prior_patents',F.coalesce(F.col('us_prior_patents'),F.lit(0.0)))
                 .withColumn('us_prior_us_citations',F.coalesce(F.col('us_prior_us_citations'),F.lit(0.0)))
                 .withColumn('us_prior_distinct_rcids',F.coalesce(F.col('us_prior_distinct_rcids'),F.lit(0)).cast('double'))
                 .persist(StorageLevel.MEMORY_AND_DISK))

    fy0 = fy.select(F.col('firm_rcid').alias('fy_firm'),F.col('year').cast('int').alias('fy_year'),F.col('firm_patents').cast('double').alias('fy_patents'),F.col('firm_us_citations').cast('double').alias('fy_citations'))
    flows=[]; desc=[]; regtabs=[]; diags=[]; serialtabs=[]
    for tag,(start,end) in WINDOWS.items():
        max_entry = end - 5
        win = candidate.filter((F.col('first_founder_entry_year')>=start)&(F.col('first_founder_entry_year')<=max_entry))
        keys = win.select('analysis_link_id','user_id','founded_rcid','first_founder_entry_year')
        direct = (keys.join(fy0, keys.founded_rcid==fy0.fy_firm,'left')
                  .filter((F.col('fy_year')>=F.col('first_founder_entry_year')+1)&(F.col('fy_year')<=F.col('first_founder_entry_year')+5)&(F.col('fy_year')>=start)&(F.col('fy_year')<=end))
                  .groupBy('analysis_link_id').agg(F.countDistinct('fy_year').alias('n_y1_5_years'),F.sum(F.col('fy_patents').isNull().cast('int')).alias('null_p'),F.sum(F.col('fy_citations').isNull().cast('int')).alias('null_c'),F.sum('fy_patents').alias('patents_y1_5'),F.sum('fy_citations').alias('citations_y1_5')))
        a = (win.join(direct,'analysis_link_id','left')
             .filter((F.col('n_y1_5_years')==5)&(F.coalesce(F.col('null_p'),F.lit(999))==0)&(F.coalesce(F.col('null_c'),F.lit(999))==0))
             .withColumn('patent_mismatch',(F.abs(F.col('firm_patents_y1_5').cast('double')-F.col('patents_y1_5'))>1e-8).cast('int'))
             .withColumn('citation_mismatch',(F.abs(F.col('firm_us_citations_y1_5').cast('double')-F.col('citations_y1_5'))>1e-8).cast('int'))
             .withColumn('log1p_patents_y1_5',F.log1p('patents_y1_5'))
             .withColumn('log1p_citations_y1_5',F.log1p('citations_y1_5'))
             .withColumn('formal_immigrant_proxy',F.col(PRIMARY_IMMIGRATION).cast('double'))
             .withColumn('has_us_prehistory',(F.col('us_prior_years_observed')>0).cast('int'))
             .withColumn('us_log1p_prior_patents',F.log1p(F.col('us_prior_patents')))
             .withColumn('us_log1p_prior_citations',F.log1p(F.col('us_prior_us_citations')))
             .withColumn('us_log1p_prior_distinct_rcids',F.log1p(F.col('us_prior_distinct_rcids')))
             .persist(StorageLevel.MEMORY_AND_DISK))
        checks = a.agg(F.count('*').alias('n_links'),F.countDistinct('founded_rcid').alias('n_firms'),F.countDistinct('user_id').alias('n_founders'),F.sum('patent_mismatch').alias('patent_mismatches'),F.sum('citation_mismatch').alias('citation_mismatches')).first().asDict()
        if int(checks['patent_mismatches'] or 0)!=0 or int(checks['citation_mismatches'] or 0)!=0:
            raise RuntimeError(f'{tag}: stored/direct y1--y5 mismatch under window restriction: {checks}')
        teams = a.groupBy('founded_rcid').agg(F.countDistinct('user_id').alias('n_core_founders_firm'))
        a = a.join(teams,'founded_rcid','left').withColumn('team_weight',F.lit(1.0)/F.col('n_core_founders_firm'))
        # Persist a bounded reproducible panel, not raw source data.
        a.select(*[c for c in ['analysis_link_id','user_id','founded_rcid','first_founder_entry_year','patents_y1_5','citations_y1_5','us_prior_years_observed','us_prior_patents','us_prior_us_citations','us_prior_distinct_rcids','founder_stem_at_founder','founder_seniority','formal_immigrant_proxy','team_weight','founder_naics3','founder_metro_area','founder_mapped_role_v3'] if c in a.columns]).write.mode('errorifexists').parquet(str(panels/f'analysis_panel_{tag}'))
        pd_a = a.toPandas()
        a.unpersist()
        flows.append({'window':tag,'outcome_calendar_start':start,'outcome_calendar_end':end,'entry_year_start':start,'entry_year_end':max_entry,'deduplicated_user_firm_links':int(n_dedup),'core_y0_5_complete_links':int(n_core_eligible),'entry_us_year_and_us_educated_links':int(n_us_edu_entry),'window_links_with_complete_y1_5':int(checks['n_links']),'window_firms':int(checks['n_firms']),'window_founders':int(checks['n_founders']),'stored_direct_patent_mismatches':int(checks['patent_mismatches'] or 0),'stored_direct_citation_mismatches':int(checks['citation_mismatches'] or 0),'observed_us_prehistory_links':int((pd_a['has_us_prehistory']==1).sum())})
        if pd_a.empty:
            continue
        # Dedicated descriptive lines for exact restricted sample.
        for trait in ['founder_stem_at_founder','formal_immigrant_proxy','has_us_prehistory']:
            if trait in pd_a:
                for g,sub in pd_a.groupby(trait,dropna=False):
                    ww=sub['team_weight'].to_numpy(float)
                    desc.append({'window':tag,'trait':trait,'group':str(g),'n_links':len(sub),'weighted_mean_patents_y1_5':float(np.average(sub['patents_y1_5'],weights=ww)),'weighted_mean_citations_y1_5':float(np.average(sub['citations_y1_5'],weights=ww)),'weighted_mean_log1p_patents_y1_5':float(np.average(sub['log1p_patents_y1_5'],weights=ww)),'weighted_mean_log1p_citations_y1_5':float(np.average(sub['log1p_citations_y1_5'],weights=ww))})
        # Models: entry traits for full sample; histories only for US-panel observed sub-sample.
        full = pd_a.copy(); histpd = pd_a.loc[pd_a['has_us_prehistory']==1].copy()
        specs = [
            ('A_us_edu_entry_traits',full,['founder_seniority'],['formal_immigrant_proxy','founder_stem_at_founder'],['first_founder_entry_year','founder_naics3']),
            ('B_us_edu_history_location_role',histpd,['us_log1p_prior_patents','us_log1p_prior_citations','us_log1p_prior_distinct_rcids','us_prior_years_observed','founder_seniority'],['formal_immigrant_proxy','founder_stem_at_founder'],['first_founder_entry_year','founder_naics3','founder_metro_area','founder_mapped_role_v3']),
            ('C_us_edu_prior_patents_only',histpd,['us_log1p_prior_patents','us_log1p_prior_distinct_rcids','us_prior_years_observed','founder_seniority'],['formal_immigrant_proxy','founder_stem_at_founder'],['first_founder_entry_year','founder_naics3','founder_metro_area','founder_mapped_role_v3']),
            ('D_us_edu_prior_citations_only',histpd,['us_log1p_prior_citations','us_log1p_prior_distinct_rcids','us_prior_years_observed','founder_seniority'],['formal_immigrant_proxy','founder_stem_at_founder'],['first_founder_entry_year','founder_naics3','founder_metro_area','founder_mapped_role_v3'])]
        for sname,df,numeric,binary,cats in specs:
            if df.empty: continue
            X,used=design_matrix(df,numeric,binary,[c for c in cats if c in df])
            for ylog,ycount,label in [('log1p_patents_y1_5','patents_y1_5','patents_y1_5'),('log1p_citations_y1_5','citations_y1_5','citations_y1_5')]:
                for estimator,fn,y in [('WLS_log1p',fit_wls,df[ylog]),('PPML',fit_ppml,df[ycount])]:
                    try:
                        tab,diag=fn(y,X,df['team_weight'],df['founded_rcid'],sname,label)
                        tab['window']=tag; tab['controls']=';'.join(used); regtabs.append(tab); diag['window']=tag; diags.append(diag)
                    except Exception as exc:
                        diags.append({'window':tag,'estimator':estimator,'sample':sname,'model':label,'error':repr(exc)})
        # Team-level earliest-entry robustness.
        te = pd_a.copy(); first=te.groupby('founded_rcid')['first_founder_entry_year'].transform('min'); te=te.loc[te['first_founder_entry_year']==first].copy()
        agg={'first_founder_entry_year':'first','patents_y1_5':'first','citations_y1_5':'first','log1p_patents_y1_5':'first','log1p_citations_y1_5':'first','founder_seniority':'mean','formal_immigrant_proxy':'mean','founder_stem_at_founder':'mean','us_log1p_prior_patents':'mean','us_log1p_prior_citations':'mean','us_log1p_prior_distinct_rcids':'mean','us_prior_years_observed':'mean'}
        for c in ['founder_naics3','founder_metro_area','founder_mapped_role_v3']:
            if c in te: agg[c]='first'
        team=te.groupby('founded_rcid',dropna=False).agg(agg).reset_index(); team['unit_weight']=1.0
        X,used=design_matrix(team,['us_log1p_prior_patents','us_log1p_prior_citations','us_log1p_prior_distinct_rcids','us_prior_years_observed','founder_seniority'],['formal_immigrant_proxy','founder_stem_at_founder'],[c for c in ['first_founder_entry_year','founder_naics3','founder_metro_area','founder_mapped_role_v3'] if c in team])
        for ylog,ycount,label in [('log1p_patents_y1_5','patents_y1_5','team_patents_y1_5'),('log1p_citations_y1_5','citations_y1_5','team_citations_y1_5')]:
            for estimator,fn,y in [('WLS_log1p',fit_wls,team[ylog]),('PPML',fit_ppml,team[ycount])]:
                try:
                    tab,diag=fn(y,X,team['unit_weight'],team['founded_rcid'],'E_us_edu_team_earliest_entry',label); tab['window']=tag; tab['controls']=';'.join(used); regtabs.append(tab); diag['window']=tag; diags.append(diag)
                except Exception as exc: diags.append({'window':tag,'estimator':estimator,'sample':'E_us_edu_team_earliest_entry','model':label,'error':repr(exc)})
        # Serial persistence under the same restrictions/window.
        sp=histpd.sort_values(['user_id','first_founder_entry_year','founded_rcid']).copy(); sp['seq']=sp.groupby('user_id').cumcount()+1; sp['n']=sp.groupby('user_id')['founded_rcid'].transform('nunique'); sp=sp.loc[sp['n']>=2].copy()
        if not sp.empty:
            first=sp.loc[sp['seq']==1].copy(); later=sp.loc[sp['seq']>=2].groupby('user_id').agg(later_pat=('log1p_patents_y1_5','mean'),later_cit=('log1p_citations_y1_5','mean'),later_year=('first_founder_entry_year','mean'))
            sf=first.merge(later,left_on='user_id',right_index=True)
            for initial,latercol,label in [('log1p_patents_y1_5','later_pat','serial_patents'),('log1p_citations_y1_5','later_cit','serial_citations')]:
                Xs,used=design_matrix(sf.assign(first_outcome=sf[initial]),['first_outcome'],[],['first_founder_entry_year','later_year'])
                tab,diag=fit_wls(sf[latercol],Xs,pd.Series(1.0,index=sf.index),sf['user_id'],'F_serial_us_edu',label);tab['window']=tag;tab['controls']=';'.join(used);serialtabs.append(tab);diag['window']=tag;diags.append(diag)
    flow=pd.DataFrame(flows); flow.to_csv(tables/'sample_flow_us_edu_windows.csv',index=False)
    pd.DataFrame(desc).to_csv(tables/'descriptive_us_edu_windows.csv',index=False)
    reg=pd.concat(regtabs,ignore_index=True) if regtabs else pd.DataFrame(); reg.to_csv(tables/'us_edu_window_regressions.csv',index=False)
    pd.DataFrame(diags).to_csv(tables/'us_edu_window_regression_diagnostics.csv',index=False)
    pd.concat(serialtabs,ignore_index=True).to_csv(tables/'serial_us_edu_window_persistence.csv',index=False) if serialtabs else pd.DataFrame().to_csv(tables/'serial_us_edu_window_persistence.csv',index=False)
    for tag in WINDOWS:
        plot_coefficients(reg,figs,tag,'WLS_log1p'); plot_coefficients(reg,figs,tag,'PPML')
    report=['# Founder-quality Phase 3 — U.S.-year, U.S.-educated compatibility sample','', '## Exact restrictions ported from the supplied lifecycle script','', f'- Source individual-year panel is limited to {INPUT_START}–{INPUT_END}.', '- An individual-year is retained only when `coalesce(last_country, first_country) = "United States"`.', '- A person-year is retained only when `last_university_country = "United States"`.', '- Outcome windows are 2000–2019 and 2010–2019; with years 1–5 post-entry outcomes, eligible observed-entry cohorts are 2000–2014 and 2010–2014.', '', '## Important scope','', '- The location restriction is implemented at the inventor-year/founder-entry and pre-entry-history level, exactly where the supplied code implements it.', '- The validated firm-year outcome panel has no verified firm-country field. Patent and citation outcomes are limited to the stated calendar windows, but are not claimed to be U.S.-firm-only.', '- Event time remains first observed founder-spell entry, not legal incorporation.', '- Pre-entry history is recomputed solely from restricted U.S.-year, U.S.-educated inventor-years and only from years strictly before observed entry.', '', '## Sample flow']
    if not flow.empty:
        for _,r in flow.iterrows(): report.append(f"- {r['window']}: {int(r['window_links_with_complete_y1_5'])} U.S.-entry/U.S.-educated core links, {int(r['window_firms'])} firms, {int(r['window_founders'])} founders; entry cohorts {int(r['entry_year_start'])}–{int(r['entry_year_end'])}.")
    report += ['', '## Outputs','', '- `tables/us_edu_window_regressions.csv`: WLS and PPML results, by legacy-compatible window.', '- `tables/sample_flow_us_edu_windows.csv`: exact restriction and cohort support.', '- `panels/analysis_panel_*`: restricted reproducible analysis panels.', '- `tables/serial_us_edu_window_persistence.csv`: descriptive serial-founder results.', '- `figures/`: selected WLS and PPML coefficient plots.']
    (run/'PHASE3_US_EDU_REPORT.md').write_text('\n'.join(report)+'\n')
    manifest={'created_utc':datetime.now(timezone.utc).isoformat(),'validation_run':str(validation),'raw_core':str(V3_CORE),'firm_year':str(V3_FIRM_YEAR),'restriction':'country_year=coalesce(last_country,first_country)==United States AND last_university_country==United States','input_years':[INPUT_START,INPUT_END],'outcome_windows':WINDOWS,'raw_us_edu_panel_rows':int(raw_us_panel_n),'flows':flows}
    (run/'manifest.json').write_text(json.dumps(manifest,indent=2,default=str)+'\n')
    print(f'[INFO] wrote Phase-3 report: {run / "PHASE3_US_EDU_REPORT.md"}', flush=True)
    spark.stop()

if __name__=='__main__': main()
