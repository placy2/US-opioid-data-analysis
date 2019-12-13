# Analysis of ARCOS Opioid Distribution Data
### Data provided by the Washington Post, analysis done by Parker Lacy in Spark.    
---
**_Background_**    

 This dataset comes from ARCOS (Automation of Reports and Consolidated Orders System), which is run by the DEA. ARCOS tracks all prescription drug distributions, both from manufacturer to distributor and distributor to buyer. This dataset in particular is a cleaned version from the Washington Post, which requires some background itself. The Washington Post was a part of a legal battle waged across America that is a broad effort to get reparations for those affected by the Opioid Crisis. In this legal battle, they acquired a court order forcing the DEA to turn up all ARCOS opioid data from 2006-2012. [WashPostOpioids](https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources "This") page is where I found the data initally, and it has quite a few useful links and resources regarding the data. Before I set out, my goals/questions were to examine the link between opioid distribution rates and other variables, specifically unemployment, from the [BLSData](https://www.bls.gov/data/ "BLS data.") I also wanted to get a better way to wrap my head around the vast amount of these drugs being shipped across the country, and whether the rates were increasing like they seemed to be or not.    
 
***    

**_Process/Findings_**    

 The first part of the process, reading the CSV file into Spark, was by far the longest, which is no surprise with data as intricate as this. The immediate realization that inferSchema did not work was the first problem, since there were 50+ columns in the dataset. The other aspect of making the schema involved trying to understand the columns themselves. The resource supplied by ARCOS itself to help understand the column conventions is a 200 page PDF with paper entry examples scanned into it, and it was last updated in the late 90s. This is indicative of how consistent the data entry was as well. After finally getting a working schema for this massive CSV, I was met with the other problem: its size and data entry conventions meant that applying null-trimming methods seemed to rid me of most of my data. After grappling with this for a while, I was finally met with some luck when I stumbled upon [ARCOS-api](https://github.com/wpinvestigative/arcos-api "this page,") which was listed as an R API on the original Washington Post article. I first found it because it has a semi-complete data dictionary with column meanings, but it turns out that regardless of being an API for R in function, this repository actually had cleaned versions of a ton of the data. These allowed me to work with more manageable chunks of data.    
 
 With this data available, I managed to get several basic stats that blew me away, especially the per capita data at the state levels:    
 * 76,630,603,021 individual pills/units were sent over the entire 7-year period
 * In 2006, there were 8,389,698,373 pills distributed.
 * In 2012, there were 12, 663,969,567 pills distributed. This is an almost 51% increase in just 6 years.
 
 * West Virginia locations purchased and sent roughly 465 pills per capita
 * Washington, D.C. locations purchased and sent the least, 91 pills per capita
 * The states with large VA distributors, SC & KS, were only 3rd and 12th in per capita rates, respectively    
 Chart of per capita rate placeholder    
 ![PerCapitaPlot](/images/PerCapitaStatePlot)
 
***    
 The next thing I attempted to do was join this data with the unemployment data provided by the BLS. To make a long story short, I never managed to get an efficient enough filter on the individual datasets to achieve this with the resources and time available to me. The unemployment values themselves were not able to be used. What were, however, that ended up being surprisingly useful, is the latitude and longitude of the pharmacies, the same info for US Counties, and county-wide populations by year. This allowed me to run several linear regressions, using some of these fields, as well as the plots below. These show the pharmacies plotted by latitude and longitude, but the second scales the points by the number of pills ordered.    
 ![LocPlot1](/images/possibleLatLonPlot)    
 ![LocPlot2](/images/biggerLatLonPlot)    
 
 Each regression attempts to predict DOSAGE_UNIT, or what is better translated to the number of individual pills of hydrocodone or oxycodone in a shipment. The first one I created used the latitude and longitude alone, to get a good baseline. This managed to achieve an average error of 16,547 pills, which isn't horrific when compared to the scale of many of these orders. Or so I thought... when I managed to get more info in, I used the county population and the year/date of the sale to try and predict this number instead. I was shocked that I was able to get an average error of only 2.56 pills. Using the same fields, I also performed a K-means clustering with a k of 3, which converged faster than any of the other k values I tried. I would theorize this is because there are 3 main distinctions between types of buyer/seller in the ARCOS data, and they often order different amounts of pills on different schedules. (For example, one of the large VA distributors in SC or KS would order more pills, more frequently than a mom-and-pop pharmacy by a factor of 10 or more.)
 



