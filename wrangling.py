import os
import pandas
flattened = pandas.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "netflix_titles.csv"))

flattened.cast = flattened.cast.str.split(',')
flattened.director = flattened.director.str.split(',')
flattened.country = flattened.country.str.split(',')
flattened.listed_in = flattened.listed_in.str.split(',')




def expand_list(df:pandas.DataFrame, column:str) -> pandas.DataFrame:
    s = df[column].str.split(',').apply(pandas.Series,1).stack()
    s.index = s.index.droplevel(-1)
    s.name = column
    return df[["show_id"]].join(s)


directors = expand_list(flattened, "director")
cast_members = expand_list(flattened, "cast")

def generate_person_table(directors:pandas.DataFrame, cast_members:pandas.DataFrame) -> pandas.DataFrame:





def get_show_to_cast_df(df:pandas.DataFrame):
    s = df.cast.str.split(',').apply(pandas.Series,1).stack()
    s.index = s.index.droplevel(-1)
    s.name = "Person" # ValueError: Other Series must have a name






