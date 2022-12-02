from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from pyspark.sql.functions import regexp_replace


class Loc:

    @classmethod
    def gu_loc(cls):
        # 서울시 자치구 정보 data
        path_gu = '/final_data/loc/서울시_자치구.csv'

        loc_data = get_spark_session().read.csv(path_gu, encoding='UTF-8', header=True)
        loc_data = loc_data.withColumnRenamed('자치구코드', 'LOC_CODE') \
                            .withColumnRenamed('자치구명', 'GU') \
                            .drop('측정소 주소')
        loc_data.show()

        # S_IDX 를 가져오기 위해 서브웨이 테이블 불러옴
        subway = find_data(DataWarehouse, 'SUBWAY')

        subway = subway.select('S_IDX', 'ADDRESS').where(subway.ADDRESS.like('%서울특별시%'))
        subway = subway.select('S_IDX', regexp_replace('ADDRESS', '서울특별시 ', '').alias('GU'))
        subway = subway.select('S_IDX', regexp_replace('GU', '\s(.*)', '').alias('GU'))
        subway.show()

        gu_df = loc_data.join(subway, on='GU')
        gu_df.show()

        # 서울시 구별 권역구분 join
        path_terr = '/final_data/loc/서울시_권역구분.csv'

        terr_data = get_spark_session().read.csv(path_terr, encoding='UTF-8', header=True)
        terr_data = terr_data.withColumnRenamed('권역구분', 'WIDE_LOC') \
                            .withColumnRenamed('자치구', 'GU')
        terr_data.show()

        gu_terr = terr_data.join(gu_df, on='GU')
        gu_terr.show()

        save_data(DataWarehouse, gu_terr, 'GU')
