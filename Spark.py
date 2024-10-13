import csv
from pyspark import SparkContext

class ProcessCSV:
    def __init__(self, file_path):
        #Initializes the file
        self.sc = SparkContext("local", "CSV RDD Example")
        self.file_path = file_path
        self.rdd = self.sc.textFile(self.file_path)
    
    def country_filter(self, country):
        #Separating the dataset into each line, and excluding the header.
        header = self.rdd.first()
        filtered_rdd = self.rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

        #Filtering for a specific country
        result_rdd = filtered_rdd.filter(lambda x: x[0].strip() == country.strip())
        return result_rdd
    
    def see_results(self, country):
        # Collect and print the results
        result_rdd=self.country_filter(country)
        results = result_rdd.count()
        return results
    
if __name__=="__main__":
    file_path="data.csv"
    processor = ProcessCSV(file_path)

    #Filter in courses
    filter = processor.country_filter('USA')
    count = filter.count()
    print(count)
