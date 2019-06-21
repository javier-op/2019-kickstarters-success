# 2019 - Successful Kickstarters
Análisis de datos de _kickstarters_ usando Apache Spark. [Daniel Araya, Javier Morales, Felipe Salgado, Grupo 4]

### Overview

El objetivo principal de este trabajo es analizar datos de proyectos *kickstarter* de manera de poder determinar y/o predecir si un nuevo proyecto de este estilo será exitoso o no. Para esto se analizaron datos de proyectos que resultaron exitosos, y se obtuvieron algunas líneas generales sobre éstos. Algunas consultas resueltas en este trabajo:
* ¿Cuál es la categoría principal en la que convergen más proyectos exitosos?
* ¿Cuáles son las categorías específicas en las que convergen más proyectos exitosos?
* ¿Cuáles son los proyectos exitosos?

### Dataset

Los datos utilizados corresponden a un _dataset_ de proyectos *kickstarter* generado a partir de diversos sitios de _crowdfunding_. Cada proyecto pertenece a una categoria (Ej: "Football") y a una categoría principal (Ej: "Sports"), además tiene una fecha de lanzamiento y una fecha de término (*deadline*), y otros campos importantes como el número de patrocinadores (_backers_), la meta en dinero a conseguir mediante el proyecto, el dinero efectivamente recaudado para el proyecto, y el resultado de éste (pudo ser exitoso, cancelado o fallido).

El tamaño del dataset es de 37MB y contiene ~379k filas.

### Metodología

Para llevar a cabo este trabajo se realizaron distintas consultas a los datos provistos utilizando la herramienta Apache Spark, ya que su motor (Java) realiza las consultas de manera más eficiente.

Las consultas consistieron en trabajos de MapReduce para obtener:
1. Las 3 categorías más exitosas (las que encajan en el mayor número de proyectos exitosos)
2. La categoría principal más exitosa (la que encaja en mayor número de proyectos exitosos)
3. Los 10 proyectos que más dinero recaudaron.
4. Número de patrocinadores por categoría

## Resultados

Los resultados obtenidos fueron los siguientes:

* La categoría principal más "exitosa" (la que incluye más proyectos exitosos) es la categoría de **música**, que tiene 24020 proyectos exitosos.

* Las 3 categorías específicas con más proyectos exitosos son:
| Categoría      	| N° de proyectos exitosos 	|
|----------------	|--------------------------	|
| Product Design 	|                     7942 	|
| Tabletop Games 	|                     7857 	|
| Shorts (film)  	|                     6625 	|

* Los 10 proyectos exitosos que recaudaron más dinero fueron:
| Proyecto                                                       	| Categoría                     	| Cantidad de dinero (USD) 	|
|----------------------------------------------------------------	|-------------------------------	|--------------------------	|
|  SCiO: Your Sixth Sense. A Pocket   Molecular Sensor For All ! 	| Technology (Hardware)         	| 27.625.572               	|
| Elite: Dangerous                                               	| Publishing (Novel)            	| 20.156.609               	|
|  Bring Back MYSTERY SCIENCETHEATER   3000                      	| Film & Video (Television)     	| 2.000.000                	|
| Camelot Unchained                                              	| Games (Video Games)           	| 2.000.000                	|
| The Veronica Mars Movie Project                                	| Film & Video (Narrative Film) 	| 2.000.000                	|
| WISH I WAS HERE                                                	| Film & Video (Narrative Film) 	| 2.000.000                	|
| Shenmue 3                                                      	| Games (Video Games)           	| 2.000.000                	|
| Blue Mountain State: The Movie                                 	| Film & Video (Narrative Film) 	| 1.500.000                	|
| The Newest Hottest Spike Lee Joint                             	| Film & Video (Narrative Film) 	| 1.250.000                	|
| The Bards Tale IV                                              	| Games (Video Games)           	| 1.250.000                	|

* Las categorías principales que cuentan con mayor número de patrocinadores:
| Categoría    	| N° de colaboradores 	|
|--------------	|---------------------	|
| Games        	| 11.331.317          	|
| Design       	| 7.239.862           	|
| Technology   	| 5.342.389           	|
| Film & Video 	| 4.176.632           	|
| Music        	| 2.694.836           	|
| Publishing   	| 2.224.635           	|
| Comics       	| 1.456.591           	|
| Fashion      	| 1.400.685           	|

## Conclusión

No todas las consultas preparadas resultaron exitosas. Algunas no pudieron ser resuetas e incluidas en este trabajo (los datos no fueron preprocesados).
Utilizar Spark ayudó para que todas las consultas fuesen más rápidas, sin embargo tiene el costo de utilizar una sintaxis verbosa y menos simple que las de otras herramientas (debido a que se usa en este caso la sintaxis de Java).
