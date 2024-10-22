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
    
    def course_filter(self):
        #Separating the dataset into each line, and excluding the header.
        header = self.rdd.first()
        filtered_rdd = self.rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

        #Filtering every specific course in each country
        filtered_course_rdd = filtered_rdd.map(lambda x: (x[0].strip(), x[-2].strip())) #Used negative indexing because the third element in CSV causes an error.
        grouped_rdd = filtered_course_rdd.groupByKey().mapValues(list)
        return grouped_rdd.collect()
    
    def pair_to_course(self):
        header = self.rdd.first()
        filtered_rdd = self.rdd.filter(lambda x: x != header).map(lambda x: x.split(","))

        #Filtering every courses
        separated_rdd = filtered_rdd.map(lambda x: (x[0].strip(), x[-1].strip(), x[-2].strip(), x[-3].strip())) #Added a filter because some are entries are lacking.
        keyed_rdd = separated_rdd.sortByKey()
        return keyed_rdd.collect()
    
    def stop_spark(self):
    #Method to stop the Spark session
        if self.sc is not None:
            self.sc.stop()
            print("Spark session stopped.")

if __name__=="__main__":
    file_path="data.csv"
    processor = ProcessCSV(file_path)

    #Know the number of observations per country
    country_input = input("Type the country: ")
    filter = processor.country_filter(country_input)
    count = filter.count()
    print(count)

    #Know every course in country
    course = processor.course_filter()
    for country, courses in course:
        print(f"COUNTRY: {country}, COURSES: {courses}")

    #Generating Key Pairs
    key_pair = processor.pair_to_course()
    for record in key_pair:
        print(f"Country: {record[0]}, Course: {record[3]}, Specialization: {record[2]}, Amount: {record[1]}")

    processor.stop_spark()