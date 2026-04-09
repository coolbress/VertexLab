# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| packages/vertex-forager/src/vertex\_forager/clients/\_\_init\_\_.py |       48 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/clients/base.py         |      166 |       28 |     83% |69, 248-250, 257-259, 271, 295-297, 304-306, 325-326, 331-333, 443-444, 520, 540-545, 558, 560-561 |
| packages/vertex-forager/src/vertex\_forager/clients/dispatcher.py   |       22 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/clients/validation.py   |        5 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/\_\_init\_\_.py    |       11 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/checkpoint.py      |      341 |       39 |     89% |148, 155, 167, 170-171, 189, 247, 315, 462-463, 486, 508-509, 521, 660, 692, 708-709, 767-768, 831-832, 847-848, 853-854, 863-864, 866-867, 884-887, 904-905, 910-911, 929 |
| packages/vertex-forager/src/vertex\_forager/core/config.py          |      209 |       16 |     92% |69, 79, 112, 115, 227, 230, 233, 236, 309, 312-313, 317, 319, 380, 401, 405 |
| packages/vertex-forager/src/vertex\_forager/core/contracts.py       |       24 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/controller.py      |      229 |       30 |     87% |121, 244, 246, 250, 254, 258, 262, 264, 283-285, 303-306, 309-315, 326-328, 350-353, 362-363 |
| packages/vertex-forager/src/vertex\_forager/core/dlq.py             |      151 |       40 |     74% |47-48, 93-94, 120-122, 136-155, 179-189, 192-193, 240-251 |
| packages/vertex-forager/src/vertex\_forager/core/errors.py          |       38 |       12 |     68% |18-19, 24-25, 105-111, 117, 123, 129 |
| packages/vertex-forager/src/vertex\_forager/core/http.py            |      105 |       26 |     75% |16-17, 100, 104-112, 163-164, 182-184, 186-193, 196-197 |
| packages/vertex-forager/src/vertex\_forager/core/library.py         |       25 |        1 |     96% |        26 |
| packages/vertex-forager/src/vertex\_forager/core/lifecycle.py       |       70 |        4 |     94% |60-61, 64-65 |
| packages/vertex-forager/src/vertex\_forager/core/orchestration.py   |       74 |       10 |     86% |43-44, 92-93, 95-97, 125, 188-189 |
| packages/vertex-forager/src/vertex\_forager/core/pipeline.py        |      916 |      105 |     89% |112-113, 141-142, 149-150, 172, 174, 179-180, 187-190, 209-216, 337-339, 426-427, 437-438, 490, 553-554, 575, 582, 591-594, 787-788, 1006-1007, 1011, 1025-1027, 1029-1032, 1099-1100, 1105-1111, 1153-1154, 1157-1158, 1161-1164, 1219, 1222-1227, 1293-1294, 1360-1380, 1475, 1488, 1546-1547, 1609-1610, 1621, 1624, 1686-1688, 1697-1698, 1757-1758, 1935-1936 |
| packages/vertex-forager/src/vertex\_forager/core/quality.py         |      115 |        5 |     96% |64, 88-89, 178-179 |
| packages/vertex-forager/src/vertex\_forager/core/registries.py      |       44 |        5 |     89% |85, 90, 113, 117, 121 |
| packages/vertex-forager/src/vertex\_forager/core/retry.py           |       94 |        6 |     94% |73, 188-191, 202 |
| packages/vertex-forager/src/vertex\_forager/core/scheduler.py       |      155 |       17 |     89% |40, 42, 44, 136-142, 144, 146-147, 159, 175, 200-201 |
| packages/vertex-forager/src/vertex\_forager/core/sweep.py           |      143 |      104 |     27% |20, 63, 67, 82-94, 103-115, 127-139, 158-200, 210-239, 251-278, 290-318, 329-347 |
| packages/vertex-forager/src/vertex\_forager/core/types.py           |       61 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/workerio.py        |       51 |        1 |     98% |       140 |
| packages/vertex-forager/src/vertex\_forager/core/writerflush.py     |      230 |       50 |     78% |109-122, 137-149, 171-174, 176, 214, 221, 257-294, 323, 340, 468-476, 479, 494, 512, 531 |
| packages/vertex-forager/src/vertex\_forager/routers/\_\_init\_\_.py |       19 |        1 |     95% |        79 |
| packages/vertex-forager/src/vertex\_forager/routers/base.py         |       23 |        1 |     96% |        65 |
| packages/vertex-forager/src/vertex\_forager/routers/errors.py       |       14 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/routers/jobs.py         |       25 |        3 |     88% |   124-127 |
| packages/vertex-forager/src/vertex\_forager/routers/transforms.py   |       43 |        5 |     88% |48, 66-67, 88-89 |
| packages/vertex-forager/src/vertex\_forager/writers/\_\_init\_\_.py |       28 |        3 |     89% |     61-66 |
| packages/vertex-forager/src/vertex\_forager/writers/base.py         |       26 |        5 |     81% |103-106, 147 |
| packages/vertex-forager/src/vertex\_forager/writers/constants.py    |       24 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/writers/duckdb.py       |      360 |       53 |     85% |141-142, 163, 239-240, 245-254, 260-266, 277, 376, 402, 408-409, 423-425, 430-431, 461-462, 536-537, 561-568, 578-582, 588, 619-629, 710-711 |
| packages/vertex-forager/src/vertex\_forager/writers/memory.py       |       64 |        8 |     88% |61, 116-122 |
| **TOTAL**                                                           | **3953** |  **578** | **85%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/coolbress/VertexLab/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/coolbress/VertexLab/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fcoolbress%2FVertexLab%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.