import pandas as pd


class ColumnCleaner:
    def __init__(self, dataframe:pd.DataFrame,
                 reset_index:bool,
                 necessary_columns:list):
        self.dataframe = dataframe
        self.reset_index = reset_index
        self.necessary_columns = necessary_columns
        
        
    def RemoveUnnecessaryColumns(self):
        try:

            # determining all unnecessary columns in data and keep them in a list
            must_remove_cols = []
            for col in self.dataframe:
                if col not in self.necessary_columns:
                    must_remove_cols.append(col)
            if len(must_remove_cols) > 0:
                try:
                    self.dataframe.drop(columns=must_remove_cols, axis=1, inplace=True)  # removing all determined columns
                    msg = 'Columns {} removed from Data successfully'.format(must_remove_cols)
                except Exception as e:
                    msg = 'Exception in removing unnecessary columns: {}'.format(e)
                    return False, msg
        except Exception as e:
            msg = 'Exception: {}'.format(e)
            return False, msg
        return True, msg
    
    
class ValueCleaner:
    
    def __init__(self, dataframe:pd.DataFrame,
                 reset_index:bool):
        self.dataframe = dataframe
        self.reset_index = reset_index
        
    def RemoveDuplicateValues(self, keep):
        """
        Check for all duplicate rows in input data and remove it

        :param data_frame: Pandas DataFrame to check
        :return: True if all task complete Successfully, otherwise False
        """
        if keep not in ['first', 'last', False]:
            raise ValueError("keep must be in ['first', 'last', False]")
        try:
            # remove all duplicate rows
            # I decided to keep 'first' row for each duplication but you can change it to 'last' if you have free time
            # BUT, don't change it to 'false' unless you want to put yourself in a self-fired situation
            self.dataframe.drop_duplicates(inplace=True, keep=keep)
            msg = 'All Duplicate rows removed successfully'
        except Exception as e:
            msg = 'Error in dropping Duplicate rows: {}'.format(e)
            return False, msg
        return True, msg
    
    
    def RemoveNanValues(self, how):
        if how not in ['any', 'all']:
            raise ValueError("keep must be in ['any', 'all']")
        try:
            # drop every rows if there is any None value in it.
            self.dataframe.dropna(how=how, inplace=True)
            msg = 'All Rows with NaN values based on how parameter removed successfully'
        except Exception as e:
            msg = 'Error in dropping NaN rows: {}'.format(e)
            return False, msg
        return True, msg