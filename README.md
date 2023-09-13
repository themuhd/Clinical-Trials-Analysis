# Clinical-Trials-Analysis
Repository contains analysis performed on clinical trials and pharmaceutical violations datasets

## The Data
1.Clinicaltrial_<year>.csv:
Each row represents an individual clinical trial, identified by an Id, listing the sponsor 
(Sponsor), the status of the study at time of the file’s download (Status), the start 
and completion dates (Start and Completion respectively), the type of study (Type), 
when the trial was first submitted (Submission), and the lists of conditions the trial 
concerns (Conditions) and the interventions explored (Interventions). Individual 
conditions and interventions are separated by commas. 
(Source: ClinicalTrials.gov)
</br>
2. pharma.csv:
The file contains a small number of a publicly available list of pharmaceutical 
violations. For the purposes of this work, we are interested in the second column, 
Parent Company, which contains the name of the pharmaceutical company in 
question. 
(Source: https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals)
