**Transaction codes:**    

Inventory Transaction Codes    
transaction code 1:Schedule Change Inventory    
transaction code 3:Year-End Inventory    
transaction code 4:Year-End In-Process Inventory (manufacturers only)    
transaction code 5:Special Inventory    
transaction code 8:No Year-End Inventory    
    
Acquisition Transaction Codes  (Increases to Inventory)    
transaction code P:Purchase or Receipt    
transaction code R:Return    
transaction code V:Unsolicited Return    
transaction code G:Government Supplied    
transaction code W:Recovered Waste  (manufacturers only)    
transaction code M:Manufactured  (manufacturers only)    
transaction code L:Reversing  (manufacturers only)    
transaction code J:Return of Sample to Inventory (manufacturers only)
    
Disposition Transaction Codes (Decreases to Inventory)    
transaction code S:Sale, Disposition, or Transfer    
transaction code Y:Destroyed    
transaction code T:Theft    
transaction code Z:Receipt by Government (seizures, samples, etc.)    
transaction code N:Nonrecoverable Waste (manufacturers only)    
transaction code U:Used in Production (manufacturers only)    
transaction code Q:Sampling  (manufacturers only)    
transaction code K:Used in Preparations (manufacturers only)    

Miscellaneous Transaction Codes    
transaction code 7:No ARCOS Activity for the Current Reporting Period     
transaction code F:Reorder DEA Form 333    
transaction code X:Lost-in-Transit    

**Action Indicator**    
code D:Delete a record    
code A:Adjust (revise) a record    
code I:Insert a record    

**NDC (National Drug Code)**    
Always coded except for transaction codes 7, 8, and F    
5 characters: Labeler code    
4 characters: Product code    
2 characters: Package size code    
Labeler and Product code are right justified with leading zeroes to ensure length, package size can be ** for bulk product    
    
**Quantity and Unit**    
For units:
1: Micrograms    
2: Milligrams    
3: Grams    
4: Kilograms    
5: Milliliters    
6: Liters    
The quantity refers to weight or volume in that unit.    
    
For units:    
D: Dozens    
K: Thousands    
The quantity refers to how many dozens or thousands of packages were shipped.