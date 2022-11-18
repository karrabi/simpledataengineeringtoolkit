import unittest
import pandas as pd


from simpledataengineeringtoolkit.checker import ValueChecker, ColumnChecker, NanValues
from simpledataengineeringtoolkit.cleaner import ColumnCleaner, ValueCleaner



class ValueCheckerTestCase(unittest.TestCase):
    
    def setUp(self):
        rows={
            'intvalues': [1,2,3,4,90],
            'floatvalues': [1.0,2.0,3.0,4.0,5.0],
            'timestampvalues': ['5544332201', '5544332202', 
                                '5544332203', '5544332204', 
                                '5544332205'],
            'datetimestringvalues': ['2022-01-01 12:01:01', '2022-01-01 12:01:02',
                                     '2022-01-01 12:01:03', '2022-01-01 12:01:04',
                                     '2022-01-01 12:01:05']
        }
        self.dataframe = pd.DataFrame(rows, index=[1,2,3,4,5])


    def test_check_integer_values_errors_to_zero(self):
        
        self.assertIn(3, list(self.dataframe['intvalues']))
        
        self.dataframe.loc[3,'intvalues'] = '3a'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckIntegerValues(column='intvalues', 
                                     change_type_to_int=True,
                                     remove=[','], 
                                     nan_values_set_to=NanValues.SetToZero)
        
        self.assertNotIn(3, list(self.dataframe['intvalues']))
        self.assertIn(0, list(self.dataframe['intvalues']))
        

    def test_check_integer_values_errors_to_Nan(self):
        
        self.dataframe.loc[3,'intvalues'] = '3a'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckIntegerValues(column='intvalues', 
                                     change_type_to_int=True,
                                     remove=[','], 
                                     nan_values_set_to=NanValues.RemainNan)
        
        self.assertNotIn(3, list(self.dataframe['intvalues']))
        self.assertTrue(self.dataframe['intvalues'].isnull().values.any())
        
    def test_check_integer_values_errors_to_Mean(self):
        
        self.dataframe.loc[4,'intvalues'] = '3a'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckIntegerValues(column='intvalues', 
                                     change_type_to_int=True,
                                     remove=[','], 
                                     nan_values_set_to=NanValues.SetToMean)
        self.assertNotIn(4, list(self.dataframe['intvalues']))
        self.assertIn(24, list(self.dataframe['intvalues']))
        
    def test_check_integer_values_errors_drop(self):
        
        self.dataframe.loc[4,'intvalues'] = '3a'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckIntegerValues(column='intvalues', 
                                     change_type_to_int=True,
                                     remove=[','], 
                                     nan_values_set_to=NanValues.DropNan)
        self.assertNotIn(4, list(self.dataframe['intvalues']))
        self.assertNotIn(4, self.dataframe['intvalues'])
    
    def test_check_float_values(self):
        self.dataframe.loc[3,'floatvalues'] = '3a'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckFloatValues(column='floatvalues', 
                                   change_type_to_float=True,
                                   remove_thousands_seperator=True,
                                   nan_values_set_to=NanValues.SetToZero
                                   )
        self.assertNotIn(3.0, list(self.dataframe['floatvalues']))
        
        
        
    def test_check_unix_timestamp_values_in_s_remove_except_digits(self):
        self.dataframe.loc[4,'timestampvalues'] = '5544a332204'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckUnixTimestampValues(column='timestampvalues',
                                           base='s',
                                           remove='*',
                                           nan_values_set_to=NanValues.RemainNan)
        self.assertIn('5544332204', list(self.dataframe['timestampvalues']))
        # self.assertTrue(self.dataframe['timestampvalues'].isnull().values.any())
        
    def test_check_unix_timestamp_values_in_s_removeLarge_remain_nan(self):
        self.dataframe.loc[4,'timestampvalues'] = '554433332204'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckUnixTimestampValues(column='timestampvalues',
                                           base='s',
                                           remove='*',
                                           nan_values_set_to=NanValues.RemainNan)
        self.assertNotIn('554433332204', list(self.dataframe['timestampvalues']))
        self.assertIn(4, self.dataframe['timestampvalues'])
        self.assertTrue(self.dataframe['timestampvalues'].isnull().values.any())
        
    def test_check_unix_timestamp_values_in_ms_removeLarge_remain_nan(self):
        self.dataframe.loc[2,'timestampvalues'] = '11111111111111'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=True)
        
        dfchecker.CheckUnixTimestampValues(column='timestampvalues',
                                           base='s',
                                           remove='*',
                                           nan_values_set_to=NanValues.RemainNan)
        self.assertNotIn('11111111111111', list(self.dataframe['timestampvalues']))
        self.assertIn(2, self.dataframe['timestampvalues'])
        self.assertTrue(self.dataframe['timestampvalues'].isnull().values.any())
        
    def test_check_unix_timestamp_values_in_s_removeLarge_drop_nan(self):
        self.dataframe.loc[4,'timestampvalues'] = '554433332204'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=False)
        
        dfchecker.CheckUnixTimestampValues(column='timestampvalues',
                                           base='s',
                                           remove='*',
                                           nan_values_set_to=NanValues.DropNan)
        self.assertNotIn('554433332204', list(self.dataframe['timestampvalues']))
        self.assertNotIn(4, self.dataframe['timestampvalues'])
        self.assertFalse(self.dataframe['timestampvalues'].isnull().values.any())
        
    def test_check_unix_timestamp_values_in_ms_removeLarge_drop_nan(self):
        self.dataframe.loc[2,'timestampvalues'] = '11111111111111'
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=False)
        dfchecker.CheckUnixTimestampValues(column='timestampvalues',
                                           base='s',
                                           remove='*',
                                           nan_values_set_to=NanValues.DropNan)
        
        self.assertNotIn('11111111111111', list(self.dataframe['timestampvalues']))
        self.assertNotIn(2, self.dataframe['timestampvalues'])
        self.assertFalse(self.dataframe['timestampvalues'].isnull().values.any())
        
    def test_check_currency_codes_values(self):
        self.dataframe['currency'] = ['BIF', 'BMD', 'BBB', 'BOB', 'BOV']
        dfchecker = ValueChecker(dataframe=self.dataframe, reset_index=False)
        dfchecker.CheckCurrencyCodes(column='currency',
                                     nan_values_set_to=NanValues.DropNan)
        
        self.assertNotIn('BBB', list(self.dataframe['currency']))
        
        
        
class ColumnCheckerTestCase(unittest.TestCase):
    def setUp(self):
        rows={
            'intvalues': [1,2,3,4,90],
            'floatvalues': [1.0,2.0,3.0,4.0,5.0],
            'timestampvalues': ['5544332201', '5544332202', 
                                '5544332203', '5544332204', 
                                '5544332205'],
            'datetimestringvalues': ['2022-01-01 12:01:01', '2022-01-01 12:01:02',
                                     '2022-01-01 12:01:03', '2022-01-01 12:01:04',
                                     '2022-01-01 12:01:05']
        }
        self.dataframe = pd.DataFrame(rows, index=[1,2,3,4,5])
        
    def test_check_necessary_columns_all_good(self):
        
        clchecker = ColumnChecker(dataframe=self.dataframe, necessary_columns=['intvalues', 
                                                                               'floatvalues', 
                                                                               'timestampvalues', 
                                                                               'datetimestringvalues'])
        self.assertTrue(clchecker.CheckNecessaryColumns()[0])
        
        
    def test_check_necessary_columns_one_missing(self):
        
        clchecker = ColumnChecker(dataframe=self.dataframe, necessary_columns=['intvalues', 
                                                                               'floatvalues', 
                                                                               'anotherfloatvalues', 
                                                                               'timestampvalues', 
                                                                               'datetimestringvalues'])
        self.assertFalse(clchecker.CheckNecessaryColumns()[0])
        
    

class ColumnCleanerTestCase(unittest.TestCase):
    def setUp(self):
        rows={
            'intvalues': [1,2,3,4,90],
            'floatvalues': [1.0,2.0,3.0,4.0,5.0],
            'timestampvalues': ['5544332201', '5544332202', 
                                '5544332203', '5544332204', 
                                '5544332205'],
            'datetimestringvalues': ['2022-01-01 12:01:01', '2022-01-01 12:01:02',
                                     '2022-01-01 12:01:03', '2022-01-01 12:01:04',
                                     '2022-01-01 12:01:05']
        }
        self.dataframe = pd.DataFrame(rows, index=[1,2,3,4,5])
        
    def test_remove_unnecessary_columns(self):
    
        clcleaner = ColumnCleaner(dataframe=self.dataframe, reset_index=False, 
                                  necessary_columns=['intvalues',
                                                     'timestampvalues',
                                                     'datetimestringvalues'])
        self.assertTrue(clcleaner.RemoveUnnecessaryColumns()[0])
        self.assertNotIn('floatvalues', self.dataframe)
        
        
    def test_remove_duplicate_data_keep_none(self):
        self.dataframe.loc[2, 'intvalues'] = 3
        self.dataframe.loc[2, 'floatvalues'] = 3.0
        self.dataframe.loc[2, 'timestampvalues'] = '5544332203'
        self.dataframe.loc[2, 'datetimestringvalues'] = '2022-01-01 12:01:03'
        
        vlcleaner = ValueCleaner(dataframe=self.dataframe, reset_index=False)
        self.assertTrue(vlcleaner.RemoveDuplicateValues(keep=False))
        self.assertNotIn(2, self.dataframe['intvalues'])
        self.assertNotIn(3, self.dataframe['intvalues'])
        
    def test_remove_duplicate_data_keep_first(self):
        self.dataframe.loc[2, 'intvalues'] = 3
        self.dataframe.loc[2, 'floatvalues'] = 3.0
        self.dataframe.loc[2, 'timestampvalues'] = '5544332203'
        self.dataframe.loc[2, 'datetimestringvalues'] = '2022-01-01 12:01:03'
        
        vlcleaner = ValueCleaner(dataframe=self.dataframe, reset_index=False)
        self.assertTrue(vlcleaner.RemoveDuplicateValues(keep='first'))
        self.assertIn(2, self.dataframe['intvalues'])
        self.assertNotIn(3, self.dataframe['intvalues'])
        
    def test_remove_duplicate_data_keep_last(self):
        self.dataframe.loc[2, 'intvalues'] = 3
        self.dataframe.loc[2, 'floatvalues'] = 3.0
        self.dataframe.loc[2, 'timestampvalues'] = '5544332203'
        self.dataframe.loc[2, 'datetimestringvalues'] = '2022-01-01 12:01:03'
        
        vlcleaner = ValueCleaner(dataframe=self.dataframe, reset_index=False)
        self.assertTrue(vlcleaner.RemoveDuplicateValues(keep='last'))
        self.assertNotIn(2, self.dataframe['intvalues'])
        self.assertIn(3, self.dataframe['intvalues'])
        
    def test_remove_nan_values_if_all(self):
        import numpy as np
        self.dataframe.loc[2, 'intvalues'] = np.nan
        self.dataframe.loc[2, 'floatvalues'] = np.nan
        self.dataframe.loc[2, 'timestampvalues'] = np.nan
        self.dataframe.loc[2, 'datetimestringvalues'] = np.nan
        self.dataframe.loc[3, 'datetimestringvalues'] = np.nan
        
        vlcleaner = ValueCleaner(dataframe=self.dataframe, reset_index=False)
        
        self.assertTrue(vlcleaner.RemoveNanValues(how='all'))
        self.assertNotIn(2, self.dataframe['intvalues'])
        self.assertIn(3, self.dataframe['intvalues'])
    
    def test_remove_nan_values_if_any(self):
        import numpy as np
        self.dataframe.loc[2, 'floatvalues'] = np.nan
        vlcleaner = ValueCleaner(dataframe=self.dataframe, reset_index=False)
        self.assertTrue(vlcleaner.RemoveNanValues(how='any'))
        self.assertNotIn(2, self.dataframe['intvalues'])