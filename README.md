# Overview

![](./public/Screen%20Shot%202022-08-12%20at%2012.39.28%20PM.png)

To run this project you need to install Astro CLI and must have Docker installed:

### via Homebrew

```
brew install astro
```

### via cURL

```
curl -sSL install.astronomer.io | sudo bash -s
```

### Install on Windows

1. Download the latest release of the Astro CLI from [this page](https://github.com/astronomer/astro-cli/releases/) into your project
2. Copy `astro.exe` somewhere in your `%PATH%`
3. Open cmd or PowerShell console and run the `astro version` command. Your output should look something like this:

   ```
   C:\Windows\system32>astro version
   Astro CLI Version: x.y.z
   ```

# Project Contents

This Astro project contains the following files and folders:

- dags: This folder contains the Python files for the Airflow DAGs. These DAGs are the main components of the project.
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- airflow_settings.yaml: This is local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

# Deploy This Project Locally

1. Start Airflow on your local machine by running:

```
astro dev start
```

This command will spin up 3 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks

- Verify that all 3 Docker containers were created by running 'docker ps'.

Airflow Webserver: http://localhost:8080
Postgres Database: localhost:5432/postgres
The default Airflow UI credentials are: admin:admin
The default Postgres DB credentials are: postgres:postgres

2. To stop the project services on your local machine, run:

```
astro dev stop
```
