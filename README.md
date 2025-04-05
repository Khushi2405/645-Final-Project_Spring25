# 645-Final-Project_Spring2025 Database Management System

# Project Description

This project is designed to process and analyze IMDB movie data using an LRU (Least Recently Used) cache for efficient data management. It reads the **IMDB dataset** (`title.basics.tsv` file), processes the movie information, and uses an LRU cache to store and retrieve data based on recency of access. The project provides a set of features to query movie data, handle cache operations, and manage large datasets efficiently.

It is built using **Java 17** and **Apache Maven 3.9.9**, with tests implemented to validate core functionalities.

## Dataset

The IMDB title.basics.tsv dataset can be downloaded from the official IMDB Developer portal:  
[IMDB Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/)

# Instructions for Running the Code

## Dependencies:
- **Java Version**: Ensure you are using **Java 17** for compatibility with the code.
- **Apache Maven**: The project requires **Apache Maven 3.9.9** for building and running tests.

## Setup Instructions:
1. **Clone the Repository**:
   Clone the repository to your local machine using the following command:
   ```bash
   git clone [https://github.com/Khushi2405/645-Final-Project_Spring25.git]
   ```
2. **Download IMDB Dataset**:
   Download the IMDB dataset (title.basics.tsv file) from the provided source.
3. **Place the IMDB Dataset**:
   After downloading the title.basics.tsv file, place it in the resources/static directory within the project.

## Building and Running the Project

### Build the Project
Open a terminal and navigate to the project directory. Run the following command to clean and build the project:

```bash
mvn clean install
```

### Running Test Cases
After building the project, run the test cases with the following command:

```bash
mvn test
```

## Running the Code in IntelliJ IDEA

### Open the Project in IntelliJ IDEA
Launch IntelliJ IDEA and open the project by navigating to **File > Open** and selecting the project directory.

### Run the Project
To run the project, right-click on the main class or test class and select **Run**.

### Ensure Dependencies
IntelliJ IDEA should automatically recognize the Maven project and download necessary dependencies. If not, you can manually refresh Maven by navigating to the **Maven** tool window and clicking on the **Refresh** button.

## For lab 2
- To use the sample dataset, index binary files and database catalogue: 
https://drive.google.com/file/d/1gaY9AdxEY1KvUNXbqbfPKshZZvMCJGLi/view?usp=sharing
Download, unzip and place it in the resources/static directory within the project.

- To create index files/binary file of dataset :
Uncomment in setup() to create index files and update total records in BTreeCorrectNessAndPerformanceTest.java

Reset database_catalogue.txt with the following values:
src/main/resources/static/data_binary_heap.bin,0,-1
src/main/resources/static/movie_id_index.bin,0,-1
src/main/resources/static/movie_title_index.bin,0,-1
 

## Notes

- Ensure that your Java environment is set to version 17 in IntelliJ IDEA by navigating to **File > Project Structure > Project > Project SDK**.
- If you face any issues with dependencies or configurations, ensure that the `pom.xml` file has been correctly set up for Java 17 and Maven 3.9.9.

## Reports
- Lab 1 report: https://docs.google.com/document/d/14tZhnIS2NrNmMGgE8UW-rLMirMoAPfPxxQ8QosYh5Gs/edit?usp=sharing
- Lab 2 report: https://docs.google.com/document/d/1w-UNgUYhvRam3E4iOgJxwrIdy_nXfXY52g8zrtHz--M/edit?usp=sharing
