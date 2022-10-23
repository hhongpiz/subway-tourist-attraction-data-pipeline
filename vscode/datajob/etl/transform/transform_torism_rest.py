from infra.jdbc import DataWarehouse, save_data
from pyspark.sql import Row
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from pyspark.sql.functions import col, count

class TourismTransformRest:

    @classmethod
    def transform(cls):
        rest = get_spark_session().read.csv('/final_data/tour_attr/강남구_맛집.csv', encoding='CP949', header=True)
        cols = "순위",'합산 검색 수'
        rest = rest.drop(*cols)
        rest
        # save_data(DataWarehouse, area_pop_fac, 'LOC')