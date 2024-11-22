import pandas as pd
from matplotlib import pyplot as plt
plt.rcParams["figure.figsize"] = [16, 9]
plt.rcParams["figure.autolayout"] = True
plt.rcParams['font.family']='Helvetica'
df = pd.read_csv("dataset.csv")
print("Contents in csv file:", df)

#Used to test the data set in the first n entries
    #df_subset = df.head(20000)
    #x = df_subset["Make"].value_counts()

#Used as the main data source
x = df.groupby(["Make", "Model Year"]).size().reset_index(name="Count")
pivot_data = x.pivot(index="Model Year", columns="Make", values="Count")
# Plot grouped bar chart
pivot_data.plot(kind="barh")


#Used in plotting the values
plt.xticks(rotation=90)
plt.xlabel("Car manufacturer")
plt.ylabel("Number of registered vehicles")
plt.title("Figure 2.1. Model years of the EVs Registered")
plt.legend(title="Car manufacturer", prop={'size': 6})
plt.show()