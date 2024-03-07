name: addressmatching_unitest
on: 
  pull_request:
    branches:
      - main
jobs:
  unittesting:
    runs-on: ubuntu-latest
    environment: devops
    steps:
      - name: Checkut code
        uses: actions/checkout@v4
      - name: Install databricks CLI
        run: |
         curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      - name: Run unittest job
        env:
          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          JOB_ID: '175058359215350'
          WAIT_TIMEOUT: '20m0s'
        run: |
          databricks jobs run-now $JOB_ID --timeout $WAIT_TIMEOUT
