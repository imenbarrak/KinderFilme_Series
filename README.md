# KinderFilme_Series
Project to gather and display information about children's films and series, including ratings and production analysis.

This project aims to gather and display comprehensive information about children's films and series, including details such as titles, ratings, and production information. Utilizing APIs from TMDb and IMDb, the application will analyze various aspects of production and realization, providing insights into trends and popular titles. The project includes features for visualizing data and facilitating further analysis.

**API Research:**
I started by identifying the necessary APIs to gather comprehensive information for analysis. Initially, the focus is on children's movies, with plans to expand to TV series and programs in the future.

**Combining APIs and Datasets:** 
I am working on integrating different APIs and datasets to minimize NaN values in the data. Although I explored web scraping, many sites restrict it, so I’m focusing on optimizing the performance of data retrieval processes, including making some processes asynchronous.

**Data Storage:**
All gathered information is stored in a MongoDB database, marking my first experience with NoSQL databases.

**Utilizing GraphQL:**
I aim to explore GraphQL in my project, specifically for interfacing with the Kitsu API.

**Visualization and UI Development:**
I plan to use frameworks like Flask, Dash, and Plotly Express to create a user interface that showcases visualizations essential for analysis.

**Incorporating FastAPI:**
FastAPI is another tool I intend to use to enhance the performance and structure of my project.

**Machine Learning Applications:**
I aim to implement machine learning techniques to answer questions such as which types of films would be most beneficial for children, while also exploring natural language processing (NLP) algorithms.

**Code Organization and Logging:**
I emphasize the importance of separation of concerns in my code to improve comprehensibility and facilitate future changes. Implementing logging is also a priority, as it helps clarify the code and understand any errors that may arise.
