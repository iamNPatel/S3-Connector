# csv conditions
class Connector:
    def __init__(self, user, file_type):
        if user == 'ml': 
            self.user = 'ml_usr'
        elif user == 'dw':
            self.user = 'dw_usr'
        else:
            return "please choose a 'dw' or 'ml' user"
        
        #avro / zip
        if file_type == 'parquet':
            self.type = 'parquet'
        elif file_type == 'csv':
            self.type = 'csv'
        else:
            return "please choose from 'parquet' or 'csv' file types"
        
    def __repr__(self):
        return f"S3 connector for {self.user}. Configured for {self.type} files"
    
    # create full s3 url with given arguments
    def create_s3_url(self, bucket, env, dataset, date, protected):
        url = self.create_s3_basepath(bucket, env, dataset, protected)
        if date != 0:
            url += f'/filedate={date}'
        return url

    # create a basepath to a s3 dataset with the given arguments
    def create_s3_basepath(self, bucket, env, dataset, protected):
        url = f's3a://cars-data-lake'
        if bucket.lower() == 'core':
            if env.lower() == 'prod':
                if protected:
                    url += f'-core-protected/{dataset.lower()}'
                else:
                    url += f'-core/{dataset.lower()}'
            else:
                url += f'-core-{env.lower()}/{dataset.lower()}'
        else:
            if env.lower() == 'prod':
                url += f'-{bucket.lower()}/{self.user}@cars.com/{dataset.lower()}'
            else:
                url += f'-{bucket.lower()}-{env.lower()}/{self.user}@cars.com/{dataset.lower()}'
        return url

    # read a single s3 partition to a df from s3 
    def read_single_partition_s3(self, bucket, env, dataset, date, protected):
        url = self.create_s3_url(bucket, env, dataset, date, protected)
        if self.type == 'parquet':
            df = spark.read.parquet(url)
            return df
        else: 
            df = spark.read.csv(url)
            return df 

    # read a range of date partitions from s3 to a df
    def read_multiple_partitions_s3(self, bucket, env, dataset, date, _range, cols, protected):
        urls = []
        start = datetime.strptime(date, "%Y-%m-%d")
        for _ in range(_range):
            url = self.create_s3_url(bucket, env, dataset, start, protected)
            urls.append(url)
            start -= timedelta(days=1)
        path = self.create_s3_basepath(bucket, env, dataset, protected)
        # Needs to specify parquet or csv
        if len(cols) > 0:
            df = spark.read.option('basePath', path).option("mergeSchema", "true").load(urls).select(cols)
        else:
            df = spark.read.option('basePath', path).option("mergeSchema", "true").load(urls)
        return df

    # parent function for reading from s3 - accepts optional arguments for range (multiple dates), cols (specific cols for partitions), and protected (bucket)
    # date = 0 for datasets without (date) partitions
    def read_from_s3(self, bucket, env, dataset, date=0, _range=1, cols=[], protected=False):
        if range == 1:
            return self.read_single_partition_s3(bucket, env, dataset, date, protected)
        else:
            return self.read_multiple_partitions_s3(bucket, env, dataset, date, _range, cols, protected)

    # write a df to s3 (landing), optional argument for number of partitions 
    def write_to_s3(self, df, bucket, env, dataset, date=0, partitions=1):
        url = self.create_s3_url(bucket, env, dataset, date)
        if self.type == 'parquet':
            df.repartition(partitions).write.parquet(url, mode='overwrite')
        else:
            df.repartition(partitions).write.mode("overwrite").csv(url)

