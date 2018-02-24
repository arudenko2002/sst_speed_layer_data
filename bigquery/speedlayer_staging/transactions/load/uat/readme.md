# Consumption transactions UAT Load

UAT load has 135 days of daily data from a date, specified as current date + all historical transactions aggregated in RTD partition. 

For example, if the current date is 2017-07-31, we load 135 daily partitions starting from 2017-03-18 and one RTD partition loaded as 2017-03-17, which will have aggregated transactions for the rest of the history. 

To launch the script, make sure that you have `umg-swift` as default project by running `gcloud init`

then run

`$ ./load_uat_mac_os.sh 'YYYY-MM-DD'` for desired date

Answer `y` to confirmation to delete `transactions` table. The script will create a new one.

Note: The script will run only on Mac OS. Some adjustments to date manipulation will need to be made on other systems. 

