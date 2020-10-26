#!/usr/bin/env python
##
# implements `expand_df` which allows us to expand a data frame column
##

import pandas as pd
import itertools


def split(data: str, separator: str):
    if data == "":
        return []
    return data.split(separator)


def join(data: list, join_char: str, max_column_split: int, missing_value: str):
    """Joins a list of data into a fix length list of data
    Examples:
        # >>> join(data=["1", "2"], join_char="|",  max_column_split=2, missing_value="NA")
        # ['1', '2']
        # >>> join(data=["1", "2","3"], join_char="|",  max_column_split=2, missing_value="NA")
        # ['1', '2|3']
        # >>> join(data=["1", "2"], join_char="|",  max_column_split=3, missing_value="NA")
        # ['1', '2', 'NA']
    Args:
        data (list): The data we wish to join
        join_char (str): The char we wish to use for the join of excess data
        max_column_split (int): The maximun length of the output array row
        missing_value (str): The value to use as replacement for missing values
    """
    normal = data[0:max_column_split - 1]
    joined = join_char.join(str(x) for x in data[max_column_split - 1:])
    if joined != "":
        joined = [joined]
    else:
        joined = []
    missing = list(itertools.repeat(
        missing_value, max_column_split - len(data)))
    return(normal + joined + missing)


def expand_df(df: pd.DataFrame, column_name: str = "", column_index: str = -1, separator: str = "|", join_char: str = "", missing_value: str = "NA", new_column_name: str = "", expand: bool = True, drop_old: bool = True, max_column_split=0, new_data_only: bool = True):
    """This function's intended usage is to expand a DataFrame's column based on a split character.
    It supports both same length splits and uneven splits, and the user has the ability to define max split level.
    Examples:
        # >>> data = [['Alex',10, "ajaja|hdsd|sds"],['Bob',12, ""],['Clarke',13, "jjs|s"]]
        # >>> df = pd.DataFrame(data,columns=['Name','Age', 'Expand'])
        # >>> print(df)
        #      Name  Age          Expand
        # 0    Alex   10  ajaja|hdsd|sds
        # 1     Bob   12
        # 2  Clarke   13           jjs|s
        # >>> expand_df(df,column_name='Expand', max_column_split=2, new_data_only=False)
        #      Name  Age Expand_0  Expand_1
        # 0    Alex   10    ajaja  hdsd|sds
        # 1     Bob   12       NA        NA
        # 2  Clarke   13      jjs         s
        # >>> expand_df(df,column_name='Expand', new_data_only=False)
        #      Name  Age Expand_0 Expand_1 Expand_2
        # 0    Alex   10    ajaja     hdsd      sds
        # 1     Bob   12       NA       NA       NA
        # 2  Clarke   13      jjs        s       NA
        # >>> expand_df(df,column_name='Expand', drop_old=False, new_data_only=False)
        #      Name  Age          Expand Expand_0 Expand_1 Expand_2
        # 0    Alex   10  ajaja|hdsd|sds    ajaja     hdsd      sds
        # 1     Bob   12                       NA       NA       NA
        # 2  Clarke   13           jjs|s      jjs        s       NA
        # >>> expand_df(df,column_index=2)
        #      Name  Age Expand_0 Expand_1 Expand_2
        # 0    Alex   10    ajaja     hdsd      sds
        # 1     Bob   12       NA       NA       NA
        # 2  Clarke   13      jjs        s       NA
        # >>> expand_df(df,column_name='Expand', expand=False, new_data_only=False)
        #      Name  Age              Expand
        # 0    Alex   10  [ajaja, hdsd, sds]
        # 1     Bob   12                  []
        # 2  Clarke   13            [jjs, s]
        # >>> expand_df(df,column_name='Expand', expand=False, new_column_name="Expanded", drop_old=False, new_data_only=False)
        #      Name  Age          Expand            Expanded
        # 0    Alex   10  ajaja|hdsd|sds  [ajaja, hdsd, sds]
        # 1     Bob   12                                  []
        # 2  Clarke   13           jjs|s            [jjs, s]
        # >>> expand_df(df,column_name='Expand', new_column_name="Expanded", drop_old=False, new_data_only=False)
        #      Name  Age          Expand Expanded_0 Expanded_1 Expanded_2
        # 0    Alex   10  ajaja|hdsd|sds      ajaja       hdsd        sds
        # 1     Bob   12                         NA         NA         NA
        # 2  Clarke   13           jjs|s        jjs          s         NA
        # >>> expand_df(df,column_index=2, max_column_split=1,separator="|", join_char=",")
        #          Expand_0
        # 0  ajaja,hdsd,sds
        # 1              NA
        # 2           jjs,s
    Args:
        df (pd.DataFrame): pandas dataframe which contains data we want to convert 
        column_name (str, optional): The pandas column name, mutually exclusive with column_index. Defaults to auto discover when column_index is defined.
        column_index (str, optional): The pandas column index, mutually exclusive with column_name. Defaults to auto discover when column_name is defined.
        separator (str, optional): The char we want to use as separator in the old dataframe. Defaults to "|".
        join_char (str, optional): The char we want to use as join char, when applicable. Defaults to `separator` value.
        missing_value (str, optional): The value we want to set as missing_value. Defaults to "NA".
        new_column_name (str, optional): The name of the new colum we want to create. Defaults to the old column_name.
        expand (bool, optional): If we want to generate multiple columns as output. Defaults to True.
        drop_old (bool, optional): If we want to drop the old column (when new_data_only is False). Defaults to True.
        max_column_split (int, optional): Limits the maximum column split (when expand is True). Use 0 to set the expand to as many as needed. Defaults to 0.
        new_data_only (bool, optional): If true it will output only the new data columns. Defaults to True.

    Returns:
        pd.DataFrame: The new processed pandas dataframe
    """
    # argument validation
    if column_name == "" and column_index == -1:
        raise ValueError(
            "you need to specify either column_name or column_index")
    if column_name != "" and column_index != -1 and column_index != df.columns.get_loc(column_name):
        raise ValueError(
            "you cannot specify both column_name and column_index")
    if max_column_split < 0:
        raise ValueError(
            "max_column_split must be greater or equal to 0")
    if column_index != -1:
        column_name = df.columns[column_index]
    else:
        column_index = df.columns.get_loc(column_name)
    if new_column_name == "":
        new_column_name = column_name
    if join_char == "":
        join_char = separator
    # create a copy of the data to avoid messing it up
    df = df.copy()
    # the actual code logic
    split_data = df.iloc[:, column_index].apply(lambda x: split(x, separator))
    if drop_old:
        df = df.drop(column_name, axis=1)
    if expand:
        max_n = max(split_data.apply(len))
        if max_column_split == 0:
            max_column_split = max_n
        name_list = ["{}_{}".format(new_column_name, x)
                     for x in list(range(0, max_column_split))]
        expanded_df = split_data.apply(lambda x: pd.Series(join(
            data=x, join_char=join_char, max_column_split=max_column_split, missing_value=missing_value), index=name_list))
        if new_data_only:
            df = expanded_df
        else:
            df = pd.concat([df, expanded_df], axis=1, sort=False)
    else:
        if new_data_only:
            df = split_data
        else:
            df[new_column_name] = split_data
    return df
