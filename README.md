# Covid Data Analysis And Real Time monitoring

## Abstract 
As the world struggles with the pandemic, It is of utmost importance that we keep people
well informed about the status of the pandemic and find patterns in the data to gain new
insights and look for solutions. This Project aims to do both, Real Time Monitoring of the
Pandemic and Analysis of the data collected so far from various popular datasets. We will
also be building models using advanced ML algorithms for various use cases like time
series forecasting and predicting the number of confirmed cases. Apache Spark along with
its libraries and Kafka will be the key technologies used.

<p align="center">
<img style="display: block; margin: auto;"
src="https://user-images.githubusercontent.com/56340004/114648391-595eaa80-9cfc-11eb-8834-cb5dcc2ccd1a.png"><br>
</p>

#### Data Analysis and ML Modelling
For the data analysis and ML modelling, we’ll be using the postman API for data collection and use Kafka to read from these api’s , process, divide and publish them to multiple topics. We’ll use spark sql for structuring the datasets and then use the pandas library for creating data frames for Modelling. The Sklearn library will be used to train our ML linear prediction model. We’ll be visualizing the predictions using the Matplotlib and seaborn library. Finally the whole application will be created as a Flask App using the Docker image of the model which will then be deployed on a Kubernetes cluster.

<p align="center">
<img width="display: block; margin: auto;" alt="Screenshot 2021-04-18 at 8 37 43 PM" src="https://user-images.githubusercontent.com/52974732/115150506-55c57d80-a086-11eb-9e6c-44edd0277760.png">
</p>
