from tiltIndicatorBefore import *

path = "input/"
# Input files
targets_ipr_raw = LoadData.get_targets_ipr_raw(path + "scenario_targets_IPR_NEW.csv")
targets_weo_raw = LoadData.get_targets_weo_raw(path + "scenario_targets_WEO_NEW.csv")
isic_tilt_mapper = LoadData.get_isic_tilt_mapper(path + "tilt_isic_mapper_2023-07-20.csv")
tilt_weo_ipr_mapper = LoadData.get_tilt_weo_ipr_mapper(path + "scenario_tilt_mapper_2023-07-20.csv")
sector_resolve = LoadData.get_sector_resolve(path + "sector_resolve_without_tiltsector.csv", path + "tilt_sector_classification.csv")
ep_companies = pd.read_csv('input/sample_ep_companies.csv')

years = [2030, 2050]
ipr = Targets(targets_ipr_raw, scenario_name = ['1.5c required policy scenario'], name_replace_dict = {'1.5c required policy scenario': '1.5C RPS'}, 
              year_filter = years).sector_profile_any_prepare_scenario()
weo = Targets(targets_weo_raw, scenario_name = ['net zero emissions by 2050 scenario'], name_replace_dict = {'net zero emissions by 2050 scenario': 'NZ 2050'}, 
              year_filter = years).sector_profile_any_prepare_scenario()
combined_scenario_targets = Targets.get_combined_targets(ipr, weo)

df_list_names = ['ep_companies', 'sector_profile_any_scenarios']
df_list = [ep_companies, combined_scenario_targets]

write_csv(df_list, df_list_names)