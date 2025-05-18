# 🎬 CS645 Final Project — IMDB Query Engine (Spring 2025)

> A disk-based database query engine with custom buffer management, B+ Tree indexing, and relational operators to execute SQL-like queries over IMDB data using Java.

---

## 📌 Overview

This project implements a custom relational query engine using Java. It operates directly over binary-encoded data pages and executes range-selection, projection, and multi-way joins over a subset of the **IMDB dataset**. The project is built incrementally through three labs:

- **Lab 1** – Binary File Management with LRU-based Buffer Manager
- **Lab 2** – B+ Tree Indexing and Search
- **Lab 3** – Query Operators and Execution Engine with Block Nested Loop Joins

---

## 🗂️ Dataset

The system works on IMDB datasets from [IMDB Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/):

| Table     | Source File               | Fields Used                                 |
|-----------|---------------------------|---------------------------------------------|
| Movies    | `title.basics.tsv`        | `tconst` (movieId), `primaryTitle` (title)  |
| WorkedOn  | `title.principals.tsv`    | `tconst`, `nconst`, `category`              |
| People    | `name.basics.tsv`         | `nconst` (personId), `primaryName` (name)   |

Each dataset is processed into fixed-size binary pages using a custom layout for efficient I/O.

---

## ⚙️ Technologies

- Java 17
- Apache Maven 3.9.9
- Apache POI (Excel Export)
- IntelliJ IDEA (recommended IDE)

---

## 🔧 Setup & Running Instructions

### 1. 📥 Clone Repository

```bash
git clone https://github.com/Khushi2405/645-Final-Project_Spring25.git
cd 645-Final-Project_Spring25
```

### 2. 📄 Download IMDB Datasets

Download the required datasets from the official IMDB Non-Commercial Datasets portal:

🔗 [IMDB Non-Commercial Datasets](https://developer.imdb.com/non-commercial-datasets/)

**Files required:**
- `title.basics.tsv` – for Movies (fields: `tconst`, `primaryTitle`)
- `title.principals.tsv` – for WorkedOn (fields: `tconst`, `nconst`, `category`)
- `name.basics.tsv` – for People (fields: `nconst`, `primaryName`)

  
### 3. 🗃️ Place Files

Put the downloaded `.tsv` files into the following directory:

```bash
src/main/resources/static/
```

### 4. 🔨 Build & Run

To build the project, open a terminal and run:

```bash
mvn clean install
```

To run all test cases
```bash
mvn test
```

### 5. ✅ IntelliJ IDEA Setup

- Open the project in **IntelliJ IDEA**
- Ensure the **Project SDK** is set to **Java 17**
  - Navigate to `File > Project Structure > Project > Project SDK`
- Refresh **Maven dependencies** using the **Maven** panel
- To execute the application:
  - Run the `DatabaseFinalProjectApplication` class for full dataset loading and initialization
  - Or run queries directly using the `UserController` class, e.g.:
    ```java
    UserController controller = new UserController(bufferSize);
    controller.runQuery("startTitle", "endTitle");
    ```



## 📦 Lab-by-Lab Breakdown

### 🧪 Lab 1: Buffer Management
- Fixed-size pages (4096 bytes) storing binary records
- Page layout includes metadata and packed byte arrays
- Buffer Manager with **LRU eviction** using a doubly linked list
- Pages marked **dirty** and **unpinned** after use
- `createPage()` and `getPage()` support efficient random access

⚠️ Assumes single-threaded access

---

### 🌳 Lab 2: B+ Tree Indexing
- Separate index files for **movie ID** and **title**
- **Catalog file** (`database_catalog.txt`) tracks file path, page count, and root
- Supports:
  - Insertion
  - Search
  - Range Search
- **Leaf** and **Non-Leaf** page formats vary by index type
- Internal **RID system** for locating records

⚠️ Indexes assume string keys and static datasets

---

### 🔁 Lab 3: Query Execution Engine
- Modular `Operator<T>` interface:
  - `ScanOperator<T>`
  - `SelectionOperator<T>`
  - `ProjectionOperator<I,O>`
  - `MaterializeOperator<T>`
  - `BNLJoinOperator<L,R,O>`
- **Block Nested Loop Join** using buffer-managed memory blocks
- **Temporary pages** with catalog indices `-1` and `-2`
- Result exported to `.xlsx` using **Apache POI**

#### 🔄 Query Plan:
1. Scan and select Movies within a title range
2. Scan, select, and project directors from WorkedOn
3. Materialize projected records
4. Join: `Movies ⨝ WorkedOn`
5. Join: result ⨝ People
6. Final projection: title and director name
7. Write results to Excel

📄 Output: `src/main/resources/static/query_output_<start>_<end>.xlsx`

---

## 🧩 Binary Files and Dataset Initialization

### 🔄 Fresh Initialization
Use these starter values in `database_catalog.txt` for clean setup:
```bash
src/main/resources/static/movie_data_binary_heap.bin,0,0
src/main/resources/static/movie_id_index.bin,0,-1
src/main/resources/static/movie_title_index.bin,0,-1
src/main/resources/static/worked_on_data_binary_heap.bin,0,0
src/main/resources/static/people_data_binary_heap.bin,0,0
src/main/resources/static/projected_worked_on_data_binary_heap.bin,0,0
```

### 📦 Use Pre-Built Dataset
If you want to skip data loading:

🔗 **[Download Binary Files (All Data)](https://drive.google.com/file/d/16K65s5gYAFPjf5FxkAQVTelWcW6bJiSh/view?usp=drive_link)**
🔗 **[Download Index Files of 10000 records(All Data)](https://drive.google.com/file/d/1gaY9AdxEY1KvUNXbqbfPKshZZvMCJGLi/view?usp=sharing)**

After downloading and extracting, place all binary files in:
```bash
src/main/resources/static/
```
 

## Notes
- Both ZIPs include a `database_catalog.txt` file.
- From the **full binary ZIP**, use all file values (e.g., `movie_data_binary_heap.bin`, `people_data_binary_heap.bin`, `worked_on_data_binary_heap.bin`, etc.).
- From the **index ZIP**, use **only** the entries related to `movie_id_index.bin` and `movie_title_index.bin`.
- This allows for quick testing on a smaller dataset containing only 10,000 records.

  
- Ensure that your Java environment is set to version 17 in IntelliJ IDEA by navigating to **File > Project Structure > Project > Project SDK**.
- If you face any issues with dependencies or configurations, ensure that the `pom.xml` file has been correctly set up for Java 17 and Maven 3.9.9.

## Reports
- Lab 1 report: https://docs.google.com/document/d/14tZhnIS2NrNmMGgE8UW-rLMirMoAPfPxxQ8QosYh5Gs/edit?usp=sharing
- Lab 2 report: https://docs.google.com/document/d/1w-UNgUYhvRam3E4iOgJxwrIdy_nXfXY52g8zrtHz--M/edit?usp=sharing
- Lab 3 report: https://docs.google.com/document/d/1w-UNgUYhvRam3E4iOgJxwrIdy_nXfXY52g8zrtHz--M/edit?usp=sharing

## 🧑‍💻 Contributors

- Khushi Gandhi  
- Vishakha Mistry

## 📝 License

This project is licensed under the MIT License.  
See the [LICENSE](LICENSE) file for details.
