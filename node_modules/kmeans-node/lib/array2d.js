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
		
	for( i = 0; i < array.length; i++)
	{
		var tmp = [];
		
		for( j = 0; j < means; j++)
		{
			if(array[i].length === 2)
				tmp[j] = distance([array[i][0],array[i][1]],values[j].values);
			else
				throw "Err: array is not of length of 2";
		}
		var min = Array.min(tmp);
		values[tmp.indexOf(min)].points.push(array[i]);
		values[tmp.indexOf(min)].sum += min;
		values[tmp.indexOf(min)].sum0 += parseFloat(array[i][0]);
		values[tmp.indexOf(min)].sum1 += parseFloat(array[i][1]);
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
		var cord = {values:[0,0],sum:0,pre:0,sum0:0,sum1:0,points:[]};

		cord.values[0] = (Math.abs(array[length*i][0]) + Math.abs(array[(length*(i+1))-1][0]))/2;
		cord.values[1] = (Math.abs(array[length*i][1]) + Math.abs(array[(length*(i+1))-1][1]))/2;
		kmeans.push(cord);
	}
	return kmeans;
}
function distance(point,mean)
{
	var x = point[0] - mean[0];
	var y = point[1] - mean[1];
	return Math.sqrt(x*x + y*y);
}
function moveMeans(means)
{
	var kmeans = [];
	for(var i = 0; i < means.length; i++)
	{
		var cord = {x:0,y:0,sum:0,sum0:0,sum1:0,pre:0,points:[]};
		cord.values[0] = means[i].sum0/means[i].points.length;
		cord.values[1] = means[i].sum1/means[i].points.length;
		cord.pre = means[i].sum;
		if(means[i].points.length == 0)
		{
			cord.values[0] = means[i].values[0];
			cord.values[1] = means[i].values[1];
		}
		kmeans.push(cord);
	}
	return kmeans;
}
Array.min = function( array )
{
    return Math.min.apply( Math, array );
}

