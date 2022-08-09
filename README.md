# Overview

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

1. Verify that all 3 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either stop your existing Docker containers or change the port.

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.
