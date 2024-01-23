import os
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

def read_azure_table(container: str, location: str, partition: str = '', columns: list = [], drop_columns: list = []) -> pd.DataFrame:
    spark_generate = SparkSession.builder.getOrCreate()
    env = 'develop'

    if partition:
        # Return the table path with the specified partition
        path = f'abfss://{container}@storagetilt{env}.dfs.core.windows.net/{location}/{partition}'
    else:
        # Return the table path without a partition
        path = f'abfss://{container}@storagetilt{env}.dfs.core.windows.net/{location}'

    storate_options = {'sas_token':'personal_sas_token'}

    if columns and drop_columns:
        raise ValueError("Arguments columns and drop_columns cannot be passed together.")

    # changed reading function
    if columns:
        df = spark_generate.read.parquet(path,storage_options=storate_options).filter(F.col('to_date')=='2099-12-31').select(columns).toPandas()
    elif drop_columns:
        df = spark_generate.read.parquet(path,storage_options=storate_options).filter(F.col('to_date')=='2099-12-31').drop(*drop_columns).toPandas()
    else:
        df = spark_generate.read.parquet(path,storage_options=storate_options).filter(F.col('to_date')=='2099-12-31').toPandas()

    return df

def write_csv(output_list, variable_names):
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        container = 'landingzone'
        output_folder = 'indicator_preparation/output'
        spark_generate = SparkSession.builder.getOrCreate()
        for df, variable_name in zip(output_list, variable_names):
            file_name = f"{variable_name}"
            file_path = f'abfss://{container}@storagetiltdevelop.dfs.core.windows.net/{output_folder}/{file_name}'
            spark_df = spark_generate.createDataFrame(df)
            spark_df.coalesce(1).write.option('header',True).mode('overwrite').parquet(file_path)
            print(f"DataFrame '{variable_name}' has been saved to {file_path}")
    else:
        output_folder = 'output'
        os.makedirs(output_folder, exist_ok=True)
        for df, variable_name in zip(output_list, variable_names):
            file_name = f"{variable_name}.csv"
            file_path = os.path.join(output_folder, file_name)
            df.to_csv(file_path, index=False)
            print(f"DataFrame '{variable_name}' has been saved to {file_path}")

class LoadData:
    """
        The class operates as a data loader that reads in the data from the /input folder and adjust columns 
        based on the file, providing a pandas dataframe as an output.
        This class only has unbound methods making it not associated to any object and is therefore only used to access methods
        as a direct data loader.
    Attributes:
        None
        
    Methods:
                    
        get_targets_ipr_raw -> pd.DataFrame:
            Returns a column-filtered dataframe of the IPR (Inevitable Policy Response) scenario targets with their corresponding information fields 
            such as sector, target area, reduction values etc.
        
        get_targets_weo_raw -> pd.DataFrame:
            Returns a column-filtered dataframe of the WEO (World Energy Outlook) scenarios including relevant information such 
            as the metrics used, the relevant year etc.
        
        get_isic_tilt_mapper -> pd.DataFrame:
            Returns a column-filtered dataframe of the mapping between the tilt sectors and the ISIC (International Standard Industrial
            Classification) 4 digit codes. 
        
        get_tilt_weo_ipr_mapper -> pd.DataFrame:
            Returns a dataframe of the mapping between the tilt subsectors, the WEO flows and IPR sectors. 
        
        get_sector_resolve -> pd.DataFrame:
            Returns a dataframe containing the clustered objects with their respective tilt sectors and subsectors.
        
    """

    @staticmethod
    def get_targets_ipr_raw(path):
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            # year has int16 should be int64
            targets_ipr_raw = read_azure_table(container = 'raw', location = 'scenario_targets_IPR_NEW').rename(columns= str.lower)[["scenario", "region", "sector", "sub sector", "year", "value"]].rename(
                columns={"sector": "ipr_sector", "sub sector": "ipr_subsector"})
        else:
        # SOURCE of formated IPR dataset used in this repo: https://docs.google.com/spreadsheets/d/1831iBQvPD_IGhNFSUQo_w8jNHzJEbcwY/edit#gid=1048759240
        # SOURCE of original IPR dataset: https://github.com/2DegreesInvesting/tiltIndicator/files/13661636/220518rpsvaluedrivers_656037.8.xlsx
            targets_ipr_raw = pd.read_csv(path).rename(columns= str.lower)[["scenario", "region", "sector", "sub sector", "year", "value"]].rename(
                columns={"sector": "ipr_sector", "sub sector": "ipr_subsector"})
        targets_ipr_raw = targets_ipr_raw.map(lambda x: x.lower() if isinstance(x, str) else x)
        targets_ipr_raw['ipr_sector'] = targets_ipr_raw['ipr_sector'].str.strip()
        targets_ipr_raw['ipr_subsector'] = targets_ipr_raw['ipr_subsector'].str.strip()
        return targets_ipr_raw

    @staticmethod
    def get_targets_weo_raw(path):
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            # year has int16 should be int64
            targets_weo_raw = read_azure_table(container = 'raw', location = 'scenario_targets_WEO_NEW').rename(columns= str.lower)[["scenario", "region", "product", "flow", "year", "value"]].rename(
                columns={"product": "weo_sector", "flow": "weo_subsector"})
        else:
        # SOURCE of formated WEO dataset used in this repo: https://docs.google.com/spreadsheets/d/1831iBQvPD_IGhNFSUQo_w8jNHzJEbcwY/edit#gid=1149756553
        # SOURCE of original WEO dataset: https://github.com/2DegreesInvesting/tiltIndicator/files/13661665/WEO2022_AnnexA_Free_Dataset_World.csv
            targets_weo_raw = pd.read_csv(path).rename(columns= str.lower)[["scenario", "region", "product", "flow", "year", "value"]].rename(
                columns={"product": "weo_sector", "flow": "weo_subsector"})
        targets_weo_raw = targets_weo_raw.map(lambda x: x.lower() if isinstance(x, str) else x)
        targets_weo_raw['weo_sector'] = targets_weo_raw['weo_sector'].str.strip()
        targets_weo_raw['weo_subsector'] = targets_weo_raw['weo_subsector'].str.strip()
        return targets_weo_raw

    @staticmethod
    def get_isic_tilt_mapper(path):
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            isic_tilt_mapper = read_azure_table(container = 'raw', location = 'tilt_isic_mapper_2023-07-20', drop_columns=['isic_4digit_name_ecoinvent', 'isic_section', 'Comments', 'from_date', 'to_date', 'tiltRecordID'])
        else:
            isic_tilt_mapper = pd.read_csv(path, encoding = 'latin-1', on_bad_lines='skip', dtype=str).drop(columns = ["isic_4digit_name_ecoinvent", "isic_section", "Comments"])
        isic_tilt_mapper = isic_tilt_mapper.map(lambda x: x.lower())
        return isic_tilt_mapper
    
    @staticmethod
    def get_tilt_weo_ipr_mapper(path):
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            mapper = read_azure_table(container = 'raw', location = 'scenario_tilt_mapper_2023-07-20', drop_columns=['from_date', 'to_date', 'tiltRecordID']).rename(columns = {'weo_product': 'weo_sector', 'weo_flow': 'weo_subsector'})
        else:
            mapper = pd.read_csv(path, sep = ',').rename(columns = {'weo_product': 'weo_sector', 'weo_flow': 'weo_subsector'})
        mapper = mapper.map(lambda x: x.lower() if isinstance(x, str) else x)
        return mapper

    @staticmethod
    def get_sector_resolve(sector_resolve_without_tiltsec, tiltsec_classification):
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            sector_resolve_without_tiltsector = read_azure_table(container = 'raw', location = 'sector_resolve_without_tiltsector', drop_columns=['from_date', 'to_date', 'tiltRecordID'])
            tilt_sector_classification = read_azure_table(container = 'raw', location = 'tilt_sector_classification', drop_columns=['from_date', 'to_date', 'tiltRecordID'])
        else:
            sector_resolve_without_tiltsector = pd.read_csv(sector_resolve_without_tiltsec)
            tilt_sector_classification = pd.read_csv(tiltsec_classification)
        sector_resolve = sector_resolve_without_tiltsector.merge(tilt_sector_classification, on = "tilt_subsector", how = "left")
        sector_resolve = sector_resolve.map(lambda x: x.lower())
        return sector_resolve

class EPCompanies:
    """
    A class for loading and merging Europages company data.

    Attributes:
        file_pattern (str): The file pattern to search for data files.
        
    Methods:
        get_ep_companies() -> pd.DataFrame:
            Load and merge all relevant EP company data files to create a unified DataFrame.

    """
    def __init__(self, file_pattern: str) -> None:
        self.path = file_pattern
        self.selected_columns = ['companies_id', 'company_name', 'country', 'company_city', 'postcode', 'address', 'main_activity', 'clustered']
        if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
            self.companies = read_azure_table(container = 'raw', location = 'companies', columns= ['companies_id', 'company_name', 'main_activity_id', 'address', 'company_city', 'postcode', 'country_id'])
            self.country = read_azure_table(container = 'raw', location = 'country', drop_columns = ['from_date', 'to_date', 'tiltRecordID'])
            self.main_activity = read_azure_table(container = 'raw', location = 'main_activity', drop_columns = ['from_date', 'to_date', 'tiltRecordID', 'ecoinvent'])
            self.products_companies = read_azure_table(container = 'raw', location = 'products_companies', drop_columns = ['from_date', 'to_date', 'tiltRecordID', 'products_companies_id'])
            self.delimited_products = read_azure_table(container = 'raw', location = 'delimited_products', drop_columns = ['from_date', 'to_date', 'tiltRecordID', 'delimited_products_id'])
            self.clustered_delimited = read_azure_table(container = 'raw', location = 'clustered_delimited', drop_columns = ['from_date', 'to_date', 'tiltRecordID', 'clustered_delimited_id'])
            self.clustered = read_azure_table(container = 'raw', location = 'clustered', drop_columns = ['from_date', 'to_date', 'tiltRecordID'])
        else:
            self.companies = pd.read_csv(self.path + "companies.csv", dtype={'postcode': str})[['companies_id', 'company_name', 'main_activity_id', 'address', 'company_city', 'postcode', 'country_id']]
            self.country = pd.read_csv(self.path + "country.csv")
            self.main_activity = pd.read_csv(self.path + "main_activity.csv")[["main_activity_id", "main_activity"]]
            self.products_companies = pd.read_csv(self.path + "products_companies.csv")[["products_id", "companies_id"]]
            self.delimited_products = pd.read_csv(self.path + "delimited_products.csv")[["delimited_id", "products_id"]]
            self.clustered_delimited = pd.read_csv(self.path + "clustered_delimited.csv")[["clustered_id", "delimited_id"]]
            self.clustered = pd.read_csv(self.path + "clustered.csv")
    
    def get_ep_companies(self) -> pd.DataFrame:
        ep_companies = (
            self.companies.merge(self.country, on='country_id')
                            .merge(self.main_activity, on='main_activity_id')
                            .merge(self.products_companies, on='companies_id')
                            .merge(self.delimited_products, on='products_id')
                            .merge(self.clustered_delimited, on='delimited_id')
                            .merge(self.clustered, on='clustered_id')
                            [self.selected_columns].drop_duplicates()
        )
        return ep_companies

    
class Targets:
    """
    A class that creates target data for IPR and WEO Scenarios.

    Attributes:
        targets (pd.DataFrame): A DataFrame containing targets data for IPR & WEO Scenarios.
        scenario_name (list): A list of scenarios to filter from target data.
        name_replace_dict (dict): A dictionary to change names of scenarios in `scenario` column.
        year_filter (list): A list of years to filter from target data.

    Methods:
        calculate_reductions() -> pd.DataFrame:
            Calculates reduction values by comparing co2 values with base year within groups of columns
    
        filter_rename_targets() -> pd.DataFrame:
            Filter and rename target data for specific scenarios and years.
        
        sector_profile_any_prepare_scenario() -> pd.DataFrame:
            Return expected output for tiltIndicator after renaming and adding columns.
        
        get_combined_targets() -> pd.DataFrame:
            Return concatenated IPR and WEO data.   
    """
    def __init__(self, targets: pd.DataFrame, scenario_name: list = [], name_replace_dict: dict = {}, year_filter: list = []) -> None:
        self.targets = targets
        self.scenario_name = scenario_name
        self.name_replace_dict = name_replace_dict
        self.year_filter = year_filter
        
    def calculate_reductions(self) -> pd.DataFrame:
        result = self.targets
        sector = result.filter(like='_sector').columns[0]
        subsector = result.filter(like='_subsector').columns[0]
        result["reductions"] = (result.sort_values(["scenario", "region", sector, subsector, "year"])
               .groupby(["scenario", "region", sector, subsector], dropna=False)[["value"]]
               .transform(lambda x: (1 - (x / x.iloc[0])))
               .round(2))
        return result
    
    def filter_rename_targets(self) -> pd.DataFrame:
        target = self.calculate_reductions()
        if self.year_filter:
            target = target[target['year'].isin(self.year_filter)]
        if self.scenario_name:
            target = target[target['scenario'].isin(self.scenario_name)]
        if self.name_replace_dict:
            target.loc[:, 'scenario'] = target.loc[:, 'scenario'].replace(self.name_replace_dict)
        return target.reset_index(drop=True)
    
    def sector_profile_any_prepare_scenario(self) -> pd.DataFrame:
        data = self.filter_rename_targets()
        sectors = data.filter(like='sector').columns
        scenario_type = list(set(col.split("_")[0] for col in sectors))[0]
        
        renamed_columns = data.rename(columns=lambda x: x.replace(scenario_type + "_", ""))
        result = pd.concat([renamed_columns, pd.DataFrame({"type": [scenario_type] * len(data)})], axis=1)
        return result
    
    def get_combined_targets(ipr, weo) -> pd.DataFrame:
        combined_targets = pd.concat([ipr, weo], axis=0).reset_index(drop=True)
        if combined_targets.reductions.dtype != 'float64':
            raise ValueError(f"`reductions` column in `{combined_targets}` is not `float64`")
        return combined_targets

