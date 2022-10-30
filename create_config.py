import configparser



def main():
    # CREATE OBJECT
    config_file = configparser.ConfigParser()
    # Add section AWS
    config_file.add_section("AWS")
    config_file.set("AWS", "KEY", '***************')
    config_file.set("AWS", "SECRET", '*******************')
    
    # ADD SECTION DWH
    config_file.add_section("DWH")
    # ADD dwh TO SECTION
    config_file.set("DWH", "HOST", "**")
    config_file.set("DWH", "DB_NAME", "dwh")
    config_file.set("DWH", "DB_USER", "dwhuser")
    config_file.set("DWH", "DB_PASSWORD", "**********")
    config_file.set("DWH", "DB_PORT", "5439")
    
    # cluster
    config_file.set("DWH", "DWH_CLUSTER_IDENTIFIER", "dwhCluster")
    config_file.set("DWH", "DWH_CLUSTER_TYPE", "multi-node")
    config_file.set("DWH", "DWH_NUM_NODES", "4")
    config_file.set("DWH", "DWH_NODE_TYPE", "dc2.large")
    
    # iam 
    config_file.set("DWH", "DWH_IAM_ROLE_NAME", "dwhRole")
    config_file.set("DWH", "ARN", "**")
    
    # datasets
    config_file.set("DWH", "LOG_DATA", 's3://udacity-dend/log_data')
    config_file.set("DWH", "LOG_JSONPATH", 's3://udacity-dend/log_json_path.json')
    config_file.set("DWH", "SONG_DATA", 's3://udacity-dend/song_data')
    

    # SAVE CONFIG FILE
    with open(r"dwh2.cfg", 'w+') as configfileObj:
        config_file.write(configfileObj)
        configfileObj.flush()
        configfileObj.close()
    
        
if __name__ == "__main__":
    main()
