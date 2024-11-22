import pandas as pd
from matplotlib import pyplot as plt
plt.rcParams["figure.figsize"] = [16, 9]
plt.rcParams["figure.autolayout"] = True
plt.rcParams['font.family']='Helvetica'
df = pd.read_csv("dataset.csv")
print("Contents in csv file:", df)

#Used to test the data set in the first 200 entries
    #df_subset = df.head(2000)
    #x = df_subset["Make"].value_counts()

#Used as the main data source
x = df["Make"].value_counts()

#Used in plotting the values
plt.xticks(rotation=90)
plt.xlabel("Car manufacturer")
plt.ylabel("Number of registered vehicles")
plt.title("Figure 2.0. Number of Car Registrations per Brand")
plt.bar(x.index, x.values)
plt.show()