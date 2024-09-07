import polars as pl

lf_1 = pl.scan_csv('../files/free_agents/final/fa_ids_complete.csv')
lf_2 = pl.scan_csv('../files/free_agents/final/fa_ids_complete_18.csv')

combined_lf = pl.concat([lf_1, lf_2])
combined_lf = combined_lf.unique(subset=['full_name', 'team', 'fa_class'])

combined_lf = combined_lf.with_columns(
    internal_id=pl.col('full_name').rank(method='ordinal').cast(pl.String)
)

combined_lf.collect().write_csv('fas_final.csv')