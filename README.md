# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| packages/vertex-forager/src/vertex\_forager/clients/\_\_init\_\_.py |       43 |        1 |     98% |       234 |
| packages/vertex-forager/src/vertex\_forager/clients/base.py         |      199 |       46 |     77% |68, 72, 86, 367-369, 376-378, 397-398, 401-405, 456-467, 488-500, 511-512, 627, 644-649, 657-667 |
| packages/vertex-forager/src/vertex\_forager/clients/dispatcher.py   |       21 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/clients/validation.py   |        5 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/\_\_init\_\_.py    |       11 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/checkpoint.py      |      222 |       68 |     69% |124, 136, 139-140, 154, 261-262, 279, 294-295, 401-404, 420-423, 432-433, 474, 496-515, 528-529, 541-564, 581-584, 593-605 |
| packages/vertex-forager/src/vertex\_forager/core/config.py          |      250 |       21 |     92% |76, 86, 121, 124, 226, 229, 232, 235, 353, 365, 368-369, 373, 375, 389, 395-396, 402-403, 459, 478 |
| packages/vertex-forager/src/vertex\_forager/core/contracts.py       |       21 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/controller.py      |      208 |       37 |     82% |121, 247, 249, 257, 261, 265, 267, 281-283, 301-304, 307-313, 357-359, 382-385, 390-399 |
| packages/vertex-forager/src/vertex\_forager/core/dlq.py             |      168 |       23 |     86% |47-48, 97-98, 120-121, 146-148, 174-175, 210-220, 282-284, 289-292 |
| packages/vertex-forager/src/vertex\_forager/core/errors.py          |       38 |       12 |     68% |18-19, 24-25, 105-111, 117, 123, 129 |
| packages/vertex-forager/src/vertex\_forager/core/http.py            |      107 |       26 |     76% |16-17, 105, 109-117, 168-169, 187-189, 191-198, 201-202 |
| packages/vertex-forager/src/vertex\_forager/core/library.py         |       25 |        1 |     96% |        26 |
| packages/vertex-forager/src/vertex\_forager/core/lifecycle.py       |       82 |       10 |     88% |33-38, 71-72, 76-77, 124, 137, 179, 187 |
| packages/vertex-forager/src/vertex\_forager/core/orchestration.py   |       74 |       10 |     86% |43-44, 92-93, 95-97, 125, 188-189 |
| packages/vertex-forager/src/vertex\_forager/core/pipeline.py        |      761 |      118 |     84% |109-110, 136-137, 144-145, 241-243, 311-312, 324-325, 329, 380, 389-406, 430, 461-462, 677-679, 685-686, 694-695, 780-782, 797-798, 802, 816-818, 820-823, 863-864, 867, 870, 890-891, 896-902, 944-945, 948-949, 952-955, 1010, 1013-1018, 1066, 1075-1076, 1078-1079, 1145-1157, 1246-1247, 1257-1258, 1285-1287, 1303-1304, 1309, 1312, 1344, 1373-1374, 1382, 1441-1442, 1451-1452, 1618-1619 |
| packages/vertex-forager/src/vertex\_forager/core/quality.py         |       90 |        4 |     96% |120-121, 228-229 |
| packages/vertex-forager/src/vertex\_forager/core/recover.py         |      114 |       18 |     84% |42-45, 89, 94, 144-146, 156-158, 178, 199, 221-224 |
| packages/vertex-forager/src/vertex\_forager/core/registries.py      |       44 |        5 |     89% |79, 84, 107, 111, 115 |
| packages/vertex-forager/src/vertex\_forager/core/retry.py           |       94 |        6 |     94% |73, 188-191, 202 |
| packages/vertex-forager/src/vertex\_forager/core/scheduler.py       |       85 |        9 |     89% |35, 78, 104-108, 113-114 |
| packages/vertex-forager/src/vertex\_forager/core/sweep.py           |      143 |      104 |     27% |20, 64, 68, 83-95, 104-116, 128-140, 159-201, 211-240, 252-279, 291-319, 330-348 |
| packages/vertex-forager/src/vertex\_forager/core/types.py           |       61 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/workerio.py        |       53 |        1 |     98% |       144 |
| packages/vertex-forager/src/vertex\_forager/core/writerflush.py     |      259 |       36 |     86% |108-114, 117-118, 135-147, 212, 219, 261-271, 321, 339, 454-476, 478, 493, 600-608, 611, 625, 632 |
| packages/vertex-forager/src/vertex\_forager/routers/\_\_init\_\_.py |       19 |        1 |     95% |        79 |
| packages/vertex-forager/src/vertex\_forager/routers/base.py         |       23 |        1 |     96% |        65 |
| packages/vertex-forager/src/vertex\_forager/routers/errors.py       |       14 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/routers/jobs.py         |       25 |        3 |     88% |   124-127 |
| packages/vertex-forager/src/vertex\_forager/routers/transforms.py   |       43 |        5 |     88% |48, 66-67, 88-89 |
| packages/vertex-forager/src/vertex\_forager/writers/\_\_init\_\_.py |       26 |        3 |     88% |     58-63 |
| packages/vertex-forager/src/vertex\_forager/writers/base.py         |       26 |        5 |     81% |103-106, 147 |
| packages/vertex-forager/src/vertex\_forager/writers/constants.py    |       24 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/writers/duckdb.py       |      308 |       41 |     87% |141-142, 163, 242-243, 251-257, 313, 325-326, 348-350, 355-356, 386-387, 461-462, 486-493, 503-507, 513, 544-554, 636-637 |
| packages/vertex-forager/src/vertex\_forager/writers/memory.py       |       60 |        8 |     87% |56, 111-118 |
| **TOTAL**                                                           | **3746** |  **623** | **83%** |           |


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