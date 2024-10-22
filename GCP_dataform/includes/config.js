// Description: Configuration file for the dataform project
const dataset_dest_name = dataform.projectConfig.vars.destination_dataset

const tables = [{
        name: "data_by_province",
        source: "jit-training-dma.bit_test_dataform.src_data_by_province",
        update_mode: "incremental",
        keys: ["date", "province_code"]
    },
    {
        name: "data_by_region",
        source: "jit-training-dma.bit_test_dataform.src_data_by_region",
        update_mode: "incremental",
        keys: ["date", "region_code"]
    },
    {
        name: "national_trends",
        source: "jit-training-dma.bit_test_dataform.src_national_trends",
        update_mode: "table",
        keys: []
    }
];

const running_date = "PARSE_DATE('%Y%m%d','" + dataform.projectConfig.vars.processing_date + "')"

const delete_partition_query = (table_name) => {
    return `
    DECLARE table_exists BOOL;

    SET table_exists = EXISTS (
    SELECT 1
    FROM \`jit-training-dma.${dataset_dest_name}.INFORMATION_SCHEMA.TABLES\`
    WHERE table_name = '${table_name}'
    );

    IF table_exists THEN
    DELETE FROM \`jit-training-dma.${dataset_dest_name}.${table_name}\`
    WHERE DATE(LOAD_TS) = ${running_date};
    END IF;
  `
}

const log_events = (table_name, status) => {
    return `
    INSERT INTO \`jit-training-dma.${dataset_dest_name}.log_events\`
    VALUES ("${table_name}","${status}",CURRENT_TIMESTAMP())
    `
}


module.exports = {
    dataset_dest_name,
    tables,
    running_date,
    delete_partition_query,
    log_events
};
