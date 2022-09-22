from infra.jdbc import DataWarehouse, save_data
from pyspark.sql import Row
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

class CoronaVaccineTransformer:
    file_name = '/corona_data/vaccine/corona_vaccine_' + cal_std_day(1) + '.json'

    @classmethod
    def transform(cls):
        vaccine = get_spark_session().read.json(cls.file_name, multiLine=True)
        data = cls.generate_rows(vaccine)                
        vaccine_data = get_spark_session().createDataFrame(data)
        vaccine_data = cls.__stack_dataframe(vaccine_data)
        save_data(DataWarehouse, vaccine_data, 'CORONA_VACCINE')

    @classmethod
    def __stack_dataframe(cls, vaccine_data):
        pd_vaccine = vaccine_data.to_pandas_on_spark()
        pd_vaccine = pd_vaccine.set_index(['loc','std_day'])
        pd_vaccine = pd_vaccine.stack()
        pd_vaccine = pd_vaccine.to_dataframe('V_CNT')

        pd_vaccine = pd_vaccine.reset_index()
        pd_vaccine = pd_vaccine.rename(columns={'level_2':'V_TH'})
        vaccine_data = pd_vaccine.to_spark()
        vaccine_data.drop('level_0')
        vaccine_data.drop('index')
        
        return vaccine_data

    @classmethod
    def generate_rows(cls, vaccine):
        data = []

        for r1 in vaccine.select(vaccine.data, vaccine.meta.std_day).toLocalIterator():
            for r2 in r1.data:
                temp = r2.asDict()
                temp['std_day'] = r1['meta.std_day']
                data.append(Row(**temp))
        return data