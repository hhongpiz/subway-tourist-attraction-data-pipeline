from pyspark.sql.functions import col, ceil
from infra.jdbc import DataMart, DataWarehouse, find_data, save_data
class CoPopuDensity:

    @classmethod
    def save(cls):
        popu = find_data(DataWarehouse, 'loc')
        patients = find_data(DataWarehouse, 'corona_patients')
        pop_patients = cls.__generate_data(popu, patients)
        save_data(DataMart, pop_patients, 'CO_POPU_DENSITY')

    @classmethod
    def __generate_data(cls, popu, patients):
        pop_patients = popu.join(patients, on='loc') \
                    .select('loc'
                           ,ceil(col('population')/col('area')).alias('POPU_DENSITY')
                            ,'qur_rate'
                            ,'std_day') \
                    .orderBy(col('std_day'))
                    
        return pop_patients
