const tables = config.tables
const running_date = config.running_date
const dataset_dest_name = config.dataset_dest_name


tables.forEach(table => {
    publish(`silver_${table.name}`, {
            type: table.update_mode,
            tags: ["bronze_to_silver",  "main_pipeline"],
            schema: dataset_dest_name,
            uniqueKey: table.keys
        })
        .query(ctx => `
      SELECT 
        *
      FROM ${ctx.ref("bronze_"+table.name)}
      WHERE DATE(LOAD_TS) = ${running_date}
    `)    
    .postOps(
      ctx => config.log_events("silver_"+table.name, "OK")
    );
});
