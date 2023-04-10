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
    
    
    def create_s3_url(self, bucket, env, table, partition, protected):
        url = 's3a://cars-data-lake'
        if bucket.lower() == 'core':
            if env.lower() == 'prod':
                if protected:
                    url += f'-core-protected/{table}/filedate={partition}'
                else:
                    url += f'-core/{table}/filedate={partition}'
            else:
                url += f'-core-{env.lower()}/{table}/filedate={partition}'
            return url
        else:
            if env.lower() == 'prod':
                url += f'-{bucket}/{self.user}@cars.com/{table}/filedate={partition}'
            else:
                url += f'-{bucket}-{env.lower()}/{self.user}@cars.com/{table}/filedate={partition}'
            return url

    def create_s3_basepath(self, bucket, env, table, protected):
        url = f's3a://cars-data-lake'
        if bucket.lower() == 'core':
            if env.lower() == 'prod':
                if protected:
                    url += f'-core-protected/{table}'
                else:
                    url += f'-core/{table}'
            else:
                url += f'-core-{env.lower()}/{table}'
        else:
            if env.lower() == 'prod':
                url += f'-{bucket}/{self.user}@cars.com/{table}'
        return url

    def read_single_partition_s3(self, bucket, env, table, partition, protected):
        url = self.create_s3_url(bucket, env, table, partition, protected)
        if self.type == 'parquet':
            df = spark.read.parquet(url)
            return df
        else: 
            df = spark.read.csv(url)
            return df 

    def read_multiple_partitions_s3(self, bucket, env, table, partition, _range, cols, protected):
        urls = []
        start = datetime.strptime(partition, "%Y-%m-%d")
        for _ in range(_range):
            url = self.create_s3_url(bucket, env, table, start, protected)
            urls.append(url)
            start -= timedelta(days=1)
        path = self.create_s3_basepath(bucket, env, table, protected)
        if len(cols) > 0:
            df = spark.read.option('basePath', path).option("mergeSchema", "true").load(urls).select(cols)
        else:
            df = spark.read.option('basePath', path).option("mergeSchema", "true").load(urls)
        return df

    def read_from_s3(self, bucket, env, table, partition, _range=1, cols=[], protected=False):
        if range == 1:
            return self.read_single_partition_s3(bucket, env, table, partition, protected)
        else:
            return self.read_multiple_partitions_s3(bucket, env, table, partition, _range, cols, protected)

    def write_to_s3(self, df, bucket, env, table, partition, partitions=1):
        url = self.create_s3_url(bucket, env, table, partition)
        if self.type == 'parquet':
            df.repartition(partitions).write.parquet(url, mode='overwrite')
        else:
            df.repartition(partitions).write.mode("overwrite").csv(url)

