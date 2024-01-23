import pandas as pd
from tiltIndicatorBefore import *
import unittest

class TestTRCompanies(unittest.TestCase):
    def test_nulls_in_clustered(self):
        """
        Test that `clustered` column can't have any null value
        """
        ep_companies_sample = pd.DataFrame({'companies_id': ["a"], 
                                            'company_name': ["a"], 
                                            'country': ["a"], 
                                            'company_city': ["a"], 
                                            'postcode': ["a"],
                                            'address': ["a"], 
                                            'main_activity': ["a"], 
                                            'clustered': [pd.NA]})
        
        mapper_ep_ei_sample = pd.DataFrame({'country': ["a"], 
                                            'main_activity': ["a"], 
                                            'clustered': [pd.NA], 
                                            'activity_uuid_product_uuid': ["a"], 
                                            'multi_match': [True], 
                                            'completion': ["a"]})
        
        activities_ids_sample = pd.DataFrame({'activity_uuid_product_uuid': ["a"], 
                                              'Activity UUID': ["a"], 
                                              'Activity Name': ["a"], 
                                              'Geography': ["a"], 
                                              'Reference Product Name': ["a"], 
                                              'Unit': ["a"], 
                                              'isic_4digit': ["a"]})
        
        isic_tilt_mapper_sample = pd.DataFrame({'tilt_sector': ["a"], 
                                                'tilt_subsector': ["a"], 
                                                'isic_4digit': ["a"]})
        
        sector_resolve_sample = pd.DataFrame({'main_activity': ["a"], 
                                              'clustered': ["a"], 
                                              'tilt_sector': ["a"], 
                                              'tilt_subsector': ["a"]})
        
        tilt_weo_ipr_mapper_sample = pd.DataFrame({'tilt_sector': ["a"], 
                                                   'tilt_subsector': ["a"], 
                                                   'weo_sector': ["a"], 
                                                   'weo_subsector': ["a"],
                                                   'ipr_sector': ["a"], 
                                                   'ipr_subsector': ["a"]})
        
        with self.assertRaises(ValueError):
            TRCompanies(ep_companies_sample, mapper_ep_ei_sample, activities_ids_sample, isic_tilt_mapper_sample, sector_resolve_sample, tilt_weo_ipr_mapper_sample).get_tr_companies()
            
    def test_multiple_activity_uuid_product_uuid(self):
        """
        Test that `activity_uuid_product_uuid` can't have more than one unique value for group of columns: ('companies_id', 'country', 'main_activity', 'clustered')
        """
        ep_companies_sample = pd.DataFrame({'companies_id': ["comp"], 
                                            'company_name': ["a"], 
                                            'country': ["coun"], 
                                            'company_city': ["a"], 
                                            'postcode': ["a"],
                                            'address': ["a"], 
                                            'main_activity': ["ma"], 
                                            'clustered': ["cl"]})
        
        mapper_ep_ei_sample = pd.DataFrame({'country': ["coun", "coun"], 
                                            'main_activity': ["ma", "ma"], 
                                            'clustered': ["cl", "cl"], 
                                            'activity_uuid_product_uuid': ["uuid1", "uuid2"], 
                                            'multi_match': [True, True], 
                                            'completion': ["a", "a"]})
        
        activities_ids_sample = pd.DataFrame({'activity_uuid_product_uuid': ["uuid1", "uuid2"], 
                                              'Activity UUID': ["a", "a"], 
                                              'Activity Name': ["a", "a"], 
                                              'Geography': ["a", "a"], 
                                              'Reference Product Name': ["a", "a"], 
                                              'Unit': ["a", "a"], 
                                              'isic_4digit': ["a", "b"]})
        
        isic_tilt_mapper_sample = pd.DataFrame({'tilt_sector': ["sec"], 
                                                'tilt_subsector': ["a"], 
                                                'isic_4digit': ["a"]})
        
        sector_resolve_sample = pd.DataFrame({'main_activity': ["ma"], 
                                              'clustered': ["cl"], 
                                              'tilt_sector': ["a"], 
                                              'tilt_subsector': ["a"]})

        tilt_weo_ipr_mapper_sample = pd.DataFrame({'tilt_sector': ["sec"], 
                                                   'tilt_subsector': ["a"], 
                                                   'weo_sector': ["a"], 
                                                   'weo_subsector': ["a"],
                                                   'ipr_sector': ["a"], 
                                                   'ipr_subsector': ["a"]})
        
        with self.assertRaises(ValueError):
            TRCompanies(ep_companies_sample, mapper_ep_ei_sample, activities_ids_sample, isic_tilt_mapper_sample, sector_resolve_sample, tilt_weo_ipr_mapper_sample).get_tr_companies()
            
    def test_multiple_sectors_in_tiltsectors(self):
        """
        Test that `tilt_sector` can't have more than one unique value for group of columns: ('companies_id', 'country', 'main_activity', 'clustered')
        """
        ep_companies_sample = pd.DataFrame({'companies_id': ["comp"], 
                                            'company_name': ["a"], 
                                            'country': ["coun"], 
                                            'company_city': ["a"], 
                                            'postcode': ["a"],
                                            'address': ["a"], 
                                            'main_activity': ["ma"], 
                                            'clustered': ["cl"]})
        
        mapper_ep_ei_sample = pd.DataFrame({'country': ["coun"], 
                                            'main_activity': ["ma"], 
                                            'clustered': ["cl"], 
                                            'activity_uuid_product_uuid': ["uuid"], 
                                            'multi_match': [True], 
                                            'completion': ["a"]})

        activities_ids_sample = pd.DataFrame({'activity_uuid_product_uuid': ["uuid"], 
                                                'Activity UUID': ["a"], 
                                                'Activity Name': ["a"], 
                                                'Geography': ["a"], 
                                                'Reference Product Name': ["a"], 
                                                'Unit': ["a"], 
                                                'isic_4digit': ["a"]})

        isic_tilt_mapper_sample = pd.DataFrame({'tilt_sector': ["sec1", "sec2"], 
                                                'tilt_subsector': ["a", "a"], 
                                                'isic_4digit': ["a", "a"]})

        sector_resolve_sample = pd.DataFrame({'main_activity': ["ma"], 
                                                'clustered': ["cl"], 
                                                'tilt_sector': ["a"], 
                                                'tilt_subsector': ["a"]})

        tilt_weo_ipr_mapper_sample = pd.DataFrame({'tilt_sector': ["sec1", "sec2"], 
                                                    'tilt_subsector': ["a", "a"], 
                                                    'weo_sector': ["a", "a"], 
                                                    'weo_subsector': ["a", "a"],
                                                    'ipr_sector': ["a", "a"], 
                                                    'ipr_subsector': ["a", "a"]})

        with self.assertRaises(ValueError):
            TRCompanies(ep_companies_sample, mapper_ep_ei_sample, activities_ids_sample, isic_tilt_mapper_sample, sector_resolve_sample, tilt_weo_ipr_mapper_sample).get_tr_companies()
                
    def test_multiple_sectors_in_isic_4digit(self):
        """
        Test that `isic_4digit` can't have more than one unique value for group of columns: ('companies_id', 'country', 'main_activity', 'clustered')
        """
        ep_companies_sample = pd.DataFrame({'companies_id': ["comp"], 
                                            'company_name': ["a"], 
                                            'country': ["coun"], 
                                            'company_city': ["a"], 
                                            'postcode': ["a"],
                                            'address': ["a"], 
                                            'main_activity': ["ma"], 
                                            'clustered': ["cl"]})
        
        mapper_ep_ei_sample = pd.DataFrame({'country': ["coun"], 
                                            'main_activity': ["ma"], 
                                            'clustered': ["cl"], 
                                            'activity_uuid_product_uuid': ["uuid"], 
                                            'multi_match': [True], 
                                            'completion': ["a"]})

        activities_ids_sample = pd.DataFrame({'activity_uuid_product_uuid': ["uuid", "uuid"], 
                                                'Activity UUID': ["a", "a"], 
                                                'Activity Name': ["a", "a"], 
                                                'Geography': ["a", "a"], 
                                                'Reference Product Name': ["a", "a"], 
                                                'Unit': ["a", "a"], 
                                                'isic_4digit': ["isic1", "isic2"]})

        isic_tilt_mapper_sample = pd.DataFrame({'tilt_sector': ["sec", "sec"], 
                                                'tilt_subsector': ["a", "a"], 
                                                'isic_4digit': ["isic1", "isic2"]})

        sector_resolve_sample = pd.DataFrame({'main_activity': ["ma"], 
                                                'clustered': ["cl"], 
                                                'tilt_sector': ["a"], 
                                                'tilt_subsector': ["a"]})

        tilt_weo_ipr_mapper_sample = pd.DataFrame({'tilt_sector': ["sec"], 
                                                    'tilt_subsector': ["a"], 
                                                    'weo_sector': ["a"], 
                                                    'weo_subsector': ["a"],
                                                    'ipr_sector': ["a"], 
                                                    'ipr_subsector': ["a"]})
        
        with self.assertRaises(ValueError):
            TRCompanies(ep_companies_sample, mapper_ep_ei_sample, activities_ids_sample, isic_tilt_mapper_sample, sector_resolve_sample, tilt_weo_ipr_mapper_sample).get_tr_companies()
            
class TestEIInputData(unittest.TestCase):
    def test_input_product_at_same_priority(self):
        """
        Test that any input product should not belong to different geographies at same priority `16`
        """
        input_data_sample = pd.DataFrame({'activityId': ["a"],
                                          'geography': ["a"],
                                          'reference product': ["a"],
                                          'exchange name': ["a"],
                                          'activityLinkId': ["a"],
                                          'activityLink_activityName': ["a"],
                                          'activityLink_geography': ["a"],
                                          'exchange unitName': ["a"], 
                                          'exchange amount': [1]})

        activities_ids_sample = pd.DataFrame({'activity_uuid_product_uuid': ["a_a", "a_a"], 
                                            'Activity UUID': ["a", "a"], 
                                            'Activity Name': ["a", "a"],
                                            'Geography': ["a", "b"], 
                                            'Reference Product Name': ["a", "a"], 
                                            'Unit': ["a", "a"], 
                                            'isic_4digit': ["a", "a"]})

        ecoinvent_inputs_overview_sample = pd.DataFrame({'exchange_uuid': ["a"],
                                                         'exchange name': ["a"]})

        input_geography_filter_sample = pd.DataFrame({'input_priority': [16, 16], 
                                                      'ecoinvent_geography': ["A", "B"]})
                
        with self.assertRaises(ValueError):
            EIInputData(input_data_sample, activities_ids_sample, ecoinvent_inputs_overview_sample, input_geography_filter_sample).get_geo_filtered_input_data()
            
    def test_input_product_at_null_input_geography(self):
        """
        Test that any input product should not have more than one NA input priority for an NA input geography
        """
        input_data_sample = pd.DataFrame({'activityId': ["a", "a"],
                                          'geography': [pd.NA, pd.NA],
                                          'reference product': ["a", "a"],
                                          'exchange name': ["a", "b"],
                                          'activityLinkId': ["a", "a"],
                                          'activityLink_activityName': ["a", "a"],
                                          'activityLink_geography': ["a", "a"],
                                          'exchange unitName': ["a", "a"], 
                                          'exchange amount': [1, 1]})

        activities_ids_sample = pd.DataFrame({'activity_uuid_product_uuid': ["a_b"], 
                                            'Activity UUID': ["a"], 
                                            'Activity Name': ["a"],
                                            'Geography': [pd.NA], 
                                            'Reference Product Name': ["a"], 
                                            'Unit': ["a"], 
                                            'isic_4digit': ["a"]})

        ecoinvent_inputs_overview_sample = pd.DataFrame({'exchange_uuid': ["a", "a"],
                                                         'exchange name': ["a", "b"]})

        input_geography_filter_sample = pd.DataFrame({'input_priority': [pd.NA], 
                                                      'ecoinvent_geography': [pd.NA]})
                
        with self.assertRaises(ValueError):
            EIInputData(input_data_sample, activities_ids_sample, ecoinvent_inputs_overview_sample, input_geography_filter_sample).get_geo_filtered_input_data()
            
class TestISTRInputs(unittest.TestCase):
    def test_multiple_input_sectors_in_tiltsectors_and_tiltsubsectors(self):
        """
        Test that `input_isic_4digit` or `input_tilt_sector` can't have more than one unique value for group of columns: ('activity_uuid_product_uuid', 'input_activity_uuid_product_uuid')
        """
        itr_products_sample = pd.DataFrame({'activity_uuid_product_uuid': ["a", "a"],
                                            'ei_activity_name': ["a", "a"],
                                            'input_activity_uuid_product_uuid': ["a", "a"], 
                                            'input_reference_product_name': ["a", "a"], 
                                            'input_unit': ["a", "a"],
                                            'input_ei_activity_name': ["a", "a"], 
                                            'input_isic_4digit': ["a", "b"], 
                                            'input_tilt_sector': ["a", "c"], 
                                            'input_tilt_subsector': ["a", "a"], 
                                            'input_weo_sector': ["a", "a"], 
                                            'input_weo_subsector': ["a", "a"], 
                                            'input_ipr_sector': ["a", "a"],
                                            'input_ipr_subsector': ["a", "a"], 
                                            'input_co2_footprint': [1.0, 1.0]})
        
        with self.assertRaises(ValueError):
            ISTRInputs(itr_products_sample).get_istr_inputs()
            
class TestTargets(unittest.TestCase):
    def test_co2_reductions_calculation(self):
        """
        Test that co2 reduction target values are calculated correctly
        """
        targets_ipr_raw_sample = pd.DataFrame({'scenario': ["a", "a", "a", "a"], 
                                            'region': ["a", "a", "a", "a"], 
                                            'ipr_sector': ["a", "a", "a", "a"], 
                                            'ipr_subsector': ["a", "a", "a", "a"], 
                                            'year': [2020, 2030, 2040, 2050], 
                                            'value': [4, 3, 2, 1]})
        
        reductions = Targets(targets_ipr_raw_sample).calculate_reductions()
        self.assertEqual(reductions["reductions"].to_list(), [0.0, 0.25, 0.5, 0.75], msg="co2 reduction target values for co2 values [4, 3, 2, 1] should be [0.0, 0.25, 0.5, 0.75]")

if __name__ == '__main__':
    unittest.main()