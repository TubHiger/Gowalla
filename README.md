# Gowalla

The Dataset link - https://snap.stanford.edu/data/loc-gowalla.html

Trendsetter Challenge
Use this dataset to answer the following
Definitions
<ul>
<li>Assume a “trendsetter” is someone who, in this dataset, visits some of the same
locations as another person, before the other person goes to that location.</li>
<li>A “trendsetter score” for a person is the number of distinct locations and people that
have visited a location after the "trendsetter".</li>
<li>A "radius of influence" for a trendsetter is an approximate measurement of the radius of
the circle that bounds the places that person has checked in. Assume the earth is flat.</li>
</ul>
Deliverables
<p>Two tables containing ten rows including the following information:</p>
<ul>
  <li>user_id of the trendsetter</li>
<li>Trendsetter score</li>
<li>Radius of influence</li>
</ul>
<p>First table displays trendsetters ranked by score, highest to lowest</p>
<p>Second table displays trendsetters ranked by radius of influence, highest to lowest</p>


<p>In the Gowalla Project, I worked on a technical data analysis task using the Stanford University dataset. I utilized Apache Spark and PySpark to handle and process the dataset efficiently. The project involved various technical aspects, including data cleaning, aggregation, and complex SQL queries to identify trendsetters based on their check-in behavior.

I implemented a system for calculating trendsetter scores and estimated the radius of influence for each trendsetter. These calculations required using geographical distance metrics on latitude and longitude data. The entire process was performed in a distributed computing environment, which allowed for scalability and efficient handling of large datasets.

The final output was presented as two tables, each containing user IDs, trendsetter scores, and radius of influence. One table ranked trendsetters by their scores, while the other ranked them by their radius of influence.

Throughout the project, I leveraged my technical skills in data processing, SQL, and data analysis, showcasing the ability to derive meaningful insights from complex datasets.</p>
