# Analysis of ARCOS Opioid Distribution Data
### Data provided by the Washington Post, analysis done by Parker Lacy in Spark.    
---
**_Background_**    

 This dataset comes from ARCOS (Automation of Reports and Consolidated Orders System), which is run by the DEA. ARCOS tracks all prescription drug distributions, both from manufacturer to distributor and distributor to buyer. This dataset in particular is a cleaned version from the Washington Post, which requires some background itself. The Washington Post was a part of a legal battle waged across America that is a broad effort to get reparations for those affected by the Opioid Crisis. In this legal battle, they acquired a court order forcing the DEA to turn up all ARCOS opioid data from 2006-2012. [WashPostOpioids](https://www.washingtonpost.com/graphics/2019/investigations/dea-pain-pill-database/#download-resources "This") page is where I found the data initally, and it has quite a few useful links and resources regarding the data. Before I set out, my goals/questions were to examine the link between opioid distribution rates and other variables, specifically unemployment, from the [BLSData](https://www.bls.gov/data/ "BLS data.") I also wanted to get a better way to wrap my head around the vast amount of these drugs being shipped across the country, and whether the rates were increasing like they seemed to be or not.    

**_Process/Findings_**    

 The first part of the process, reading the CSV file into Spark, was by far the longest, which is no surprise with data as intricate as this. The immediate realization that inferSchema did not work was the first problem, since there were 50+ columns in the dataset. After finally getting a working schema for this massive CSV, I was met with the other problem: its size and data entry conventions meant that applying null-trimming methods seemed to rid me of most of my data. After grappling with this for a while, I was finally met with some luck when I stumbled upon [ARCOS-api](https://github.com/wpinvestigative/arcos-api "this page,") which was listed as an R API on the original Washington Post article. It turns out that regardless of being an API for R in function, this repository actually had cleaned versions of a ton of the data, allowing me to work with more manageable chunks of data.    
 
 With this data available, I managed to get several basic stats that blew me away, especially the per capita data at the state levels:    
 * 76,630,603,021 individual pills/units were sent over the entire 7-year period
 * In 2006, there were 8,389,698,373 pills distributed.
 * In 2012, there were 12, 663,969,567 pills distributed. This is an almost 51% increase in just 6 years.
 
 * West Virginia locations purchased and sent roughly 465 pills per capita
 * Washington, D.C. locations purchased and sent the least, 91 pills per capita
 * The states with large VA distributors, SC & KS, were only 3rd and 12th in per capita rates, respectively    
 



