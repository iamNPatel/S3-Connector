# csv conditions
class Connector:
    def __init__(self, user):
        if user == 'ml': 
            self.user = 'ml_usr'
        elif user == 'dw':
            self.user = 'dw_usr'
        else:
            return "please choose a 'dw' or 'ml' user"
                
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
    def read_single_partition_s3(self, bucket, env, dataset, date, protected, csv=False, avro=False, zip=False):
        url = self.create_s3_url(bucket, env, dataset, date, protected)
        if csv:
            return spark.read.option("header",True).csv(url)
        elif avro:
            return spark.read.format('avro').load(url)
        elif zip:
            pass
        else:
            return spark.read.parquet(url)

    # read a range of date partitions from s3 to a df
    def read_multiple_partitions_s3(self, bucket, env, dataset, date, _range, cols, protected, csv=False, avro=False, zip=False):
        urls = []
        start = datetime.strptime(date, "%Y-%m-%d")
        for _ in range(_range):
            url = self.create_s3_url(bucket, env, dataset, start, protected)
            urls.append(url)
            start -= timedelta(days=1)
        path = self.create_s3_basepath(bucket, env, dataset, protected)
        if csv:
            return spark.read.csv([urls])
        elif avro:
            pass
        elif zip:
            pass
        else:    
            if len(cols) > 0:
                return spark.read.option('basePath', path).option("mergeSchema", "true").load(urls).select(cols)
            else:
                return spark.read.option('basePath', path).option("mergeSchema", "true").load(urls)
        

    # parent function for reading from s3 - accepts optional arguments for range (multiple dates), cols (specific cols for partitions), and protected (bucket)
    # date = 0 for datasets without (date) partitions
    def read_from_s3(self, bucket, env, dataset, date=0, _range=1, cols=[], protected=False, csv=False, avro=False, zip=False):
        if range == 1:
            return self.read_single_partition_s3(bucket, env, dataset, date, protected, csv=False, avro=False, zip=False)
        else:
            return self.read_multiple_partitions_s3(bucket, env, dataset, date, _range, cols, protected, csv=False, avro=False, zip=False)

    # write a df to s3 (landing), optional argument for number of partitions 
    def write_to_s3(self, df, bucket, env, dataset, date=0, partitions=1, csv=False, avro=False, zip=False):
        url = self.create_s3_url(bucket, env, dataset, date)
        if csv:
            df.repartition(partitions).write.mode("overwrite").csv(url)
        elif avro:
            df.write.format('avro').mode('overwrite').save(url)
        elif zip:
            # write zip file
            pass
        else:
            df.repartition(partitions).write.parquet(url, mode='overwrite')

