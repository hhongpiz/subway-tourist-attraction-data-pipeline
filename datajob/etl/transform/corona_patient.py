from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col

class CoronaPatientTransformer:
    
    @classmethod
    def transform(cls):

        path = '/corona_data/patient/corona_patient_' + cal_std_day(1) + '.json'
        co_patient_json = get_spark_session().read.json(path, encoding='UTF-8')
        co_patient_json = get_spark_session().read.json(path, encoding='UTF-8')
        data = []

        for r1 in co_patient_json.select('items').toLocalIterator():
            if not r1.items:
                continue
            for r2 in r1.items:
                data.append(r2)

            patient_data = get_spark_session().createDataFrame(data)
            patient_data.show(3)

            co_patients = patient_data.select(
                patient_data.gubun.alias('LOC')
                ,patient_data.deathCnt.alias('DEATH_CNT')
                ,patient_data.defCnt.alias('DEF_CNT')
                ,patient_data.localOccCnt.alias('LOC_OCC_CNT')
                ,patient_data.qurRate.alias('QUR_RATE')
                ,patient_data.stdDay.alias('STD_DAY')
            ).where(~(col('LOC').isin(['합계','검역'])) ).distinct()
            co_patients.printSchema()
            
            save_data(DataWarehouse, co_patients, 'CORONA_PATIENTS')
