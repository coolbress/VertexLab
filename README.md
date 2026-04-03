# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/coolbress/VertexLab/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| packages/vertex-forager/src/vertex\_forager/clients/\_\_init\_\_.py |       43 |        1 |     98% |       189 |
| packages/vertex-forager/src/vertex\_forager/clients/base.py         |      165 |       22 |     87% |71, 75, 311-313, 320-322, 341-342, 347-349, 459-460, 538, 555-560, 573, 575-576 |
| packages/vertex-forager/src/vertex\_forager/clients/dispatcher.py   |       21 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/clients/validation.py   |        5 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/\_\_init\_\_.py    |       11 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/checkpoint.py      |      222 |       68 |     69% |124, 136, 139-140, 154, 261-262, 279, 294-295, 401-404, 420-423, 432-433, 474, 496-515, 528-529, 541-564, 581-584, 593-605 |
| packages/vertex-forager/src/vertex\_forager/core/config.py          |      240 |       19 |     92% |75, 85, 120, 123, 217, 220, 223, 226, 345, 348-349, 353, 355, 360-361, 367-368, 424, 443 |
| packages/vertex-forager/src/vertex\_forager/core/contracts.py       |       21 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/controller.py      |      219 |       38 |     83% |121, 247, 249, 257, 261, 265, 267, 283-285, 303-306, 309-315, 355, 361-363, 387-390, 395-404 |
| packages/vertex-forager/src/vertex\_forager/core/dlq.py             |      151 |       40 |     74% |47-48, 93-94, 119-121, 135-154, 178-188, 191-192, 239-250 |
| packages/vertex-forager/src/vertex\_forager/core/errors.py          |       38 |       12 |     68% |18-19, 24-25, 105-111, 117, 123, 129 |
| packages/vertex-forager/src/vertex\_forager/core/http.py            |      107 |       26 |     76% |16-17, 105, 109-117, 168-169, 187-189, 191-198, 201-202 |
| packages/vertex-forager/src/vertex\_forager/core/library.py         |       25 |        1 |     96% |        26 |
| packages/vertex-forager/src/vertex\_forager/core/lifecycle.py       |       74 |        6 |     92% |33-38, 70-71, 74-75 |
| packages/vertex-forager/src/vertex\_forager/core/orchestration.py   |       74 |       10 |     86% |43-44, 92-93, 95-97, 125, 188-189 |
| packages/vertex-forager/src/vertex\_forager/core/pipeline.py        |      879 |      105 |     88% |114-115, 141-142, 149-150, 172, 174, 179-180, 187-190, 209-216, 341-343, 422-423, 433-434, 486, 544-545, 722-723, 944-945, 949, 963-965, 967-970, 1009-1010, 1036-1037, 1042-1048, 1090-1091, 1094-1095, 1098-1101, 1156, 1159-1164, 1230-1231, 1297-1311, 1406, 1418-1419, 1477-1478, 1540-1541, 1552, 1555, 1617-1619, 1628-1629, 1688-1689, 1698-1699, 1864-1865, 2036 |
| packages/vertex-forager/src/vertex\_forager/core/quality.py         |       90 |        4 |     96% |120-121, 228-229 |
| packages/vertex-forager/src/vertex\_forager/core/recover.py         |      114 |       18 |     84% |42-45, 89, 94, 144-146, 156-158, 178, 199, 221-224 |
| packages/vertex-forager/src/vertex\_forager/core/registries.py      |       44 |        5 |     89% |84, 89, 112, 116, 120 |
| packages/vertex-forager/src/vertex\_forager/core/retry.py           |       94 |        6 |     94% |73, 188-191, 202 |
| packages/vertex-forager/src/vertex\_forager/core/scheduler.py       |      155 |       17 |     89% |40, 42, 44, 136-142, 144, 146-147, 159, 175, 200-201 |
| packages/vertex-forager/src/vertex\_forager/core/sweep.py           |      143 |      104 |     27% |20, 63, 67, 82-94, 103-115, 127-139, 158-200, 210-239, 251-278, 290-318, 329-347 |
| packages/vertex-forager/src/vertex\_forager/core/types.py           |       61 |        0 |    100% |           |
| packages/vertex-forager/src/vertex\_forager/core/workerio.py        |       51 |        1 |     98% |       140 |
| packages/vertex-forager/src/vertex\_forager/core/writerflush.py     |      260 |       75 |     71% |108-121, 136-148, 170-173, 175, 213, 220, 256-293, 322, 340, 468-476, 479, 494, 513-530, 560, 577, 586-627, 632 |
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
| **TOTAL**                                                           | **3875** |  **645** | **83%** |           |


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