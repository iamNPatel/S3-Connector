class Connector:
    def __init__(self, user, file_type):
        if user == 'ml': 
            self.user = 'ml_usr'
        elif user == 'dw':
            self.user = 'dw_usr'
        
        else:
            return "please choose a 'dw' or 'ml' user"
        
        if file_type == 'parquet':
            self.type = 'parquet'
        elif file_type == 'csv':
            self.type = 'csv'
        else:
            return "please choose from 'parquet' or 'csv' file types"
        
    def __repr__(self):
        return f"S3 connector for {self.user}. Configured for {self.type} files"
    
    def create_s3_url(self, bucket, env, table, partition, partition_on):
        url = 's3a://cars-data-lake'
        if bucket.lower() == 'core':
            if env.lower() == 'prod':
                url += f'-core/{table}/{partition_on}={partition}'
            else:
                url += f'-core-{env.lower()}/{table}/{partition_on}={partition}'
            return url
        else:
            if env.lower() == 'prod':
                url += f'-{bucket}/{self.user}@cars.com/{table}/{partition_on}={partition}'
            else:
                url += f'-{bucket}-{env.lower()}/{self.user}@cars.com/{table}/{partition_on}={partition}'
            return url
        print('oops?')

    def read_single_partition_s3(self, bucket, env, table, partition, partition_on='filedate'):
        url = self.create_s3_url(bucket, env, table, partition, partition_on)
        if self.type == 'parquet':
            df = spark.read.parquet(url)
            return df
        else: 
            df = spark.read.csv(url)
            return df 

    def read_multiple_partitions_s3():
        pass

    def read_from_s3(self, bucket, env, table, partition, range=1, partition_on='filedate'):
        if range == 1:
            return self.read_single_partition_s3(bucket, env, table, partition, partition_on)
        else:
            return self.read_multiple_partitions_from_s3()

    def write_to_s3(self, df, bucket, env, table, partition, partition_on, partitions=1):
        url = self.create_s3_url(bucket, env, table, partition, partition_on)
        if self.type == 'parquet':
            df.repartition(partitions).write.parquet(url, mode='overwrite')
        else:
            df.repartition(partitions).write.mode("overwrite").csv(url)
