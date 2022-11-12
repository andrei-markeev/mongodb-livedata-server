export function numberToRadius(number) {
    return number * Math.PI / 180;
}

// from http://www.movable-type.co.uk/scripts/latlong.html
export function pointDistance(pt1, pt2) {
    var lon1 = pt1.coordinates[0],
        lat1 = pt1.coordinates[1],
        lon2 = pt2.coordinates[0],
        lat2 = pt2.coordinates[1],
        dLat = numberToRadius(lat2 - lat1),
        dLon = numberToRadius(lon2 - lon1),
        a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(numberToRadius(lat1))
            * Math.cos(numberToRadius(lat2)) * Math.pow(Math.sin(dLon / 2), 2),
        c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    // Earth radius is 6371 km
    return (6371 * c) * 1000; // returns meters
}

// checks if geometry lies entirely within a circle
// works with Point, LineString, Polygon
export function geometryWithinRadius(geometry, center, radius) {
    if (geometry.type == 'Point') {
        return pointDistance(geometry, center) <= radius;
    } else if (geometry.type == 'LineString' || geometry.type == 'Polygon') {
        var point: any = {};
        var coordinates;
        if (geometry.type == 'Polygon') {
            // it's enough to check the exterior ring of the Polygon
            coordinates = geometry.coordinates[0];
        } else {
            coordinates = geometry.coordinates;
        }
        for (var i in coordinates) {
            point.coordinates = coordinates[i];
            if (pointDistance(point, center) > radius) {
                return false;
            }
        }
    }
    return true;
}