# BUILDING ENERGY META MODELS

## Predicting building energy performance at city-scale. 
This repository contains the workflow developed for my MSc thesis project in Architecture, Urbanism and Building Sciences – Building Technology.
The accompanying thesis report can be accessed from the TU Delft repository using the following link:  
https://repository.tudelft.nl/record/uuid:f91540ee-aa71-43f7-afb1-d0fbe0fa3c6c


### Overview
This study investigates the application of machine learning (ML) for predicting building energy performance at city scale. 
The computational workflow is structured in two main stages: (i) large-scale building energy simulation and (ii) ML model development. 
In part (i) the scripts were developed to automate data processing, energy simulation, and analysis for 20,000 residential buildings in Rotterdam, Netherlands. Energy simulations were modelled for both the present and two projected climate conditions. The EnergyPlus simulation datasets can be provided upon request.
In part (ii) the scripts focus on structuring input features, developing and training a ML model, and evaluating model performance. 

### Repository Structure

#### Large-Scale Building Energy Simulation

1. Data Collection – Scripts for collecting building attribute data and creating building geometry.
2. Data Generation – Automated EnergyPlus input data file creation and batch simulation.
3. Demand Analysis – Processing simulated heating and cooling demand results.

#### ML Model Development

4. Data Structure – Feature engineering, dataset structuring, and storage for ML input.
5. ML Development – Neural network model training and validation (integrated with Weights & Biases).
6. Performance – Model testing, error metrics, and scenario-based performance evaluation.


### Software & Environment Setup

•	Python: 3.13  
• Editor/IDE: Developed in Visual Studio  
• Energy simulation: Requires local EnergyPlus v9.6+ installation.  
• ML training: uses Weights & Biases (wandb) for performance tracking and visualization.  
