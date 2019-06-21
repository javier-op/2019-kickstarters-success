# 2019 - Successful Kickstarters
Análisis de datos de _kickstarters_ usando Apache Spark. [Daniel Araya, Javier Morales, Felipe Salgado, Grupo 4]

### Overview

El objetivo principal de este trabajo es analizar datos de proyectos *kickstarter* de manera de poder determinar y/o predecir si un nuevo proyecto de este estilo será exitoso o no. Para esto se analizaron datos de proyectos que resultaron exitosos, y cómo influyen algunas características de cada proyecto en su resultado.

### Dataset

Los datos utilizados corresponden a un _dataset_ de proyectos *kickstarter* generado a partir de diversos sitios de _crowdfunding_. Cada proyecto pertenece a una categoria (Ej: "Football") y a una categoría principal (Ej: "Sports"), además tiene una fecha de lanzamiento y una fecha de término (*deadline*), y otros campos importantes como el número de patrocinadores (_backers_), la meta en dinero a conseguir mediante el proyecto, y el dinero efectivamente recaudado para el proyecto.

El tamaño del dataset es de 37MB y contiene ~379k filas.

### Métodos

Para llevar a cabo este trabajo se realizaron distintas consultas a los datos provistos utilizando la herramienta Apache Spark. Estas consultas consistieron en trabajos de Map-Reduce para obtener:
1. Las 3 categorías más exitosas (las que encajan en el mayor número de proyectos exitosos)
2. La categoría principal más exitosa (la que encaja en mayor número de proyectos exitosos)
3. Los 10 proyectos que más dinero recaudaron.
4. Porcentaje que cada categoría representa del dinero total recaudado
5. Número de patrocinadores por categoría
6. Relación entre la duración de cada proyecto y el dinero recaudado por éste.

<!--Detail the methods used during the project. Provide an overview of the techniques/technologies used, why you used them and how you used them. Refer to the source-code delivered with the project. Describe any problems you encountered.-->

## Resultados

Los resultados obtenidos fueron los siguientes:

Detail the results of the project. Different projects will have different types of results; e.g., run-times or result sizes, evaluation of the methods you're comparing, the interface of the system you've built, and/or some of the results of the data analysis you conducted.

## Conclusión

Summarise main lessons learnt. What was easy? What was difficult? What could have been done better or more efficiently?
