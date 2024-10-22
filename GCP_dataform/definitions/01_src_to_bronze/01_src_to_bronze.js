const tables = config.tables
const running_date = config.running_date
const dataset_dest_name = config.dataset_dest_name

tables.forEach(table => {
    publish(`bronze_${table.name}`, {
            type: "incremental",
            tags: ["src_to_bronze", "main_pipeline"],
            schema: dataset_dest_name,
            bigquery: {
                partitionBy: 'DATE(LOAD_TS)'
            }
        })
        .preOps(
            ctx => config.delete_partition_query("bronze_" + table.name))
        .query(ctx => `
      SELECT 
        *,
        TIMESTAMP(${running_date}) AS LOAD_TS
      FROM ${table.source}
    `)
        .postOps(
            ctx => config.log_events("bronze_" + table.name, "OK")
        );
});
