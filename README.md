# stochastic_risk_east_africa
This repository contains the code for the reproduction of the paper:

_Benito, I., Aerts, J.C.J.H., Eilander, D., Ward, P.J., and Muis,S., 2024. Stochastic coastal flood risk modelling for the east coast of Africa._ 

In this study we use a novel modelling framework to dynamically simulate stochastic coastal flood risk for the east coast of Africa. Our approach uses 10,000 years of synthetic tropical cyclones from STORM as input data to dynamically simulate water levels using Global Tide and Surge Model (GTSM). These water level timeseries are then used as coastal boundary conditions for the hydrodynamic flood model Super-Fast INundation of CoastS (SFINCS). Subsequently, we calculate the damage of each individual tropical cyclone event and empirically derive the risk curve for each country.

The code consists of the following files and directories:
* **py_environments:** folder containing the environments necessary to run the scripts
   * **j1_environment.yml:** environment file to run the j1_run_GTSM.sh bash script
   * **j2_environment.yml:** environment file to run the j2_run_SFINCS.sh bash script
   * **j3_environment.yml:** environment file to run the j3*.sh bash scripts
* **py_scripts:** folder containing the main scripts to execute the stochastic risk modelling 
   * **p0_tc_filtering:** folder containing the scripts to filter the TC events from STORM
       * **filter_distance2land_v2.py:** script to filter the TC events from STORM based on distance to land
       * **job_tcfiltering.sh:** bash script to run filter_distance2land_v2.py
   * **p1_holland_model:** folder containing the scripts to run the Holland model
   * **j1_run_GTSM.sh:** bash script to execute GTSM and run its pre- and postprocess
   * **j2_run_SFINCS.sh:** bash script to execute SFINCS, run its pre- and postprocess and run the impact model
   * **j3a_frequency_analysis.sh:** bash script to run the .py scripts that perform the frequency analysis
   * **j3b_event_information.sh:** bash script to run the .py scripts that derive information for each event
   * **j3c_RPs_and_plots.sh:** bash script to run the .py scripts that calculate the return periods and make the plots of the results
   * **p2_preprocess_GTSM.py:** script to prepare the model files of GTSM
   * **p3_postprocess_GTSM.py:** script to process the GTSM output
   * **p4_preprocess_SFINCS.py:** script to update the SFINCS model templates with the TC event information
   * **p5_postprocess_SFINCS_run_IMPACT.py:** script to process the SFINCS output and execute the impact model
   * **p6a_frequency_analysis.py:** script to calculate the maximum flood extent per event
   * **p6b_surge_residuals.py:** script to calculate the maximum storm surge residuals per event
   * **p6b_totalwl.py:** script to calculate the maximum total water levels per event
   * **p6b_worst_events.py:** script to calculate the build damages per event for all the study area
   * **p6b_worst_events_build_perCOUNTRY_goodzonal_v2.py:** script to calculate the build damages per event per country
   * **p6b_worst_events_pop.py:** script to calculate the affected population per event for all the study area
   * **p6b_worst_events_pop_perCOUNTRY_goodzonal_v2.py:** script to calculate the affected population per event per country
   * **p6c_RPs_ANNUAL_allarea_prl_withEVENTnrs.py:** script to calculate the return period of annual damages & affected population for all the study area
   * **p6c_RPs_ANNUAL_perCOUNTRY_withEVENTnrs.py:** script to calculate the return period of annual damages & affected population per country
   * **p6c_RPs_EVENT_allarea_prl.py:** script to calculate the return period of event damages & affected population for all the study area
   * **p6c_RPs_EVENT_flood_extent.py:** script to calculate the return period of flood extents per event
   * **p6c_RPs_EVENT_perCOUNTRY.py:** script to calculate the return period of event damages & affected population per country
   * **p6c_RPs_EVENT_storm_surge.py:** script to calculate the return period of storm surge residuals per event
   * **p6c_RPs_EVENT_water_level.py:** script to calculate the return period of total water levels per event
   * **p6c_plots_fig1.py:** script to generate Figure 1
   * **p6c_plots_fig2.py:** script to generate Figure 2
   * **p6c_plots_fig3.py:** script to generate Figure 3
 
* **sfincs_template:** folder containing the scripts to generate SFINCS model templates
    * **job_build_submodels.sh:** bash script to run build_sfincs_submodels.py
    * **build_sfincs_submodels.py:** script to generate a model template for each SFINCS model
