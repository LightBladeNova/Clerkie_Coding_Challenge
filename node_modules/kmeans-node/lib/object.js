module.exports = function kmeans(array,means)
{
	return clustering(array,means,null,0);
}

function clustering(array,means,values,iter)
{
	if(array.constructor != Array)
		throw "Err: values has to be in an array";
	if(means < 2)
		throw "Err: means number has to be 2 at least";
	if(means > array.length/2)
		throw "Err: means has to be equal or less half of the array length";
	if(values == null)
		values = _randomMeans(means,array);
		
	for(var i = 0; i < array.length; i++)
	{
		var tmp = [];
		for(var j = 0; j < means; j++)
		{
			tmp[j] = distance({x:array[i].x,y:array[i].y},{x:values[j].x,y:values[j].y});
		}
		var min = Array.min(tmp);
		values[tmp.indexOf(min)].points.push(array[i]);
		values[tmp.indexOf(min)].sum += min;
		values[tmp.indexOf(min)].sumX += parseFloat(array[i].x);
		values[tmp.indexOf(min)].sumY += parseFloat(array[i].y);
	}
	var stop = [];
	for(var i=0;i<values.length;i++)
	{
		if(values[i].sum == values[i].pre)
			stop.push(1);
		else
			stop.push(0);
	}
	if(values == null)
		return clustering(array,means,moveMeans(values));
	else if(stop.indexOf(0) > -1 || iter > 10)
	{
		return values;
	}
	else
	{
		return clustering(array,means,moveMeans(values),++iter);
	}
}

function _randomMeans(means,array)
{
	var kmeans = [];
	var length = Math.floor(array.length/means);	
	for(i=0;i < means;i++)
	{
		var cord = {x:0,y:0,sum:0,pre:0,sumX:0,sumY:0,points:[]};
		cord.x = (Math.abs(array[length*i].x) + Math.abs(array[(length*(i+1))-1].x))/2;
		cord.y = (Math.abs(array[length*i].y) + Math.abs(array[(length*(i+1))-1].y))/2;
		kmeans.push(cord);
	}
	return kmeans;
}
function distance(point,mean)
{
	var x = point.x - mean.x;
	var y = point.y - mean.y;
	return Math.sqrt(x*x + y*y);
}
function moveMeans(means)
{
	var kmeans = [];
	for(var i = 0; i < means.length; i++)
	{
		var cord = {x:0,y:0,sum:0,sumX:0,sumY:0,pre:0,points:[]};
		cord.x = means[i].sumX/means[i].points.length;
		cord.y = means[i].sumY/means[i].points.length;
		cord.pre = means[i].sum;
		if(means[i].points.length == 0)
		{
			cord.x = means[i].x;
			cord.y = means[i].y;
		}
		kmeans.push(cord);
	}
	return kmeans;
}
Array.min = function( array ){
    return Math.min.apply( Math, array );
}
