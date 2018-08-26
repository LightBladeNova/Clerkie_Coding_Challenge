# kmeans-node
A javascript implementation of k-means clustering algorithm that include one/two dimentional arrays, and supports objects as well for coordinates.

<h2> <a href="#install"> Install </a></h2>
<pre><code> npm install kmeans-node </code></pre>

<h2> <a href="#usage">Usage</a> </h2>
<h3> Kmeans with using an array of objects </h3>
<p> Possible using this method if points are releated with data, such as grouping coordinates that has certain properties such as shopping malls, cafes, etc .. , can be called by kmeans.object(arrayOfObjects,MeansNumber) </p>
<pre><code>
var kmeans = require('kmeans-node');
var array = [{x:1,y:2,data:"some data"},
             {x:3,y:4,data:"some data"},
             {x:7,y:8,data:"some data"},
             {x:9,y:10,data:"some data"},
             {x:13,y:14,data:"some data"},
             {x:15,y:16,data:"some data"},
             {x:22,y:23,data:"some data"},
             {x:24,y:25,data:"some data"}];
var object = kmeans.object(array,4);
</code></pre>

<h3> Kmeans with using a one dimentioanl array </h3>
<p>a one dimentional array clustering as below, can be called by kmeans.array(arrayOfObjects,MeansNumber) </p>
<pre><code>
var array = [1,2,4,8,10,14,18,20];
var object = kmeans.array(array,4);
</code></pre>

<h3> Kmeans with using a tow dimentioanl array </h3>
<p>a two dimentional array clustering as below, can be called by kmeans.array2d(arrayOfObjects,MeansNumber) </p>
<pre><code>
var array = [[1,2],[3,4],[7,8],[9,10],[13,14],[15,16],[22,23],[24,25]];
var object = kmeans.array2d(array,4);
</code></pre>

<h2><a href="#output">Output</a></h2>
<p> the result output of the clustering is an array of objects, as below descripes the outcome of each method

<h3> Object </h3>
<pre><code>
{   
    x: median of X,
    y: median of Y,
    sum: sum of distance,
    pre: previous distance,
    sumX: distance of X,
    sumY: distance of Y,
    points: array of mean's points 
}
</code></pre>

<h3> array </h3>
<pre><code>
{ value: median, sum: sum of distance, pre: previous distance, points: array of mean's points }
</code></pre>

<h3> two dimentioanl array </h3>
<pre><code>
{
    values: array of length 2 that contains medians,
    sum: sum of distance,
    pre: previous distance,
    sum0: distance of array[0],
    sum1: distance of array[1],
    points: array of mean's points 
}
</code></pre>

<h2><a href="#license">License</a></h2>
The MIT License (MIT)

Copyright (c) 2015 abdullahshahin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
