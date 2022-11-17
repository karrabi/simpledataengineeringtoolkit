import pandas as pd
from simpledataengineeringtoolkit.checker import ValueChecker

def main():
    df = pd.DataFrame()
    valueChecker = ValueChecker(dataframe=df, reset_index=False)

if __name__ == '__main__':
    main()
    