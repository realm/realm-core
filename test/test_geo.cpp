/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"
#ifdef TEST_GEO

#include <ostream>
#include <sstream>

#include "s2/s2cell.h"
#include "s2/s2latlng.h"
#include "s2/s2pointregion.h"
#include "s2/s2polygon.h"
#include "s2/s2regioncoverer.h"

#include "test.hpp"

using namespace realm::util;
using namespace realm::test_util;

TEST(Geo_Link)
{
    // ExpressionMapping::cover2dsphere
    S2LatLng latlng = S2LatLng::FromDegrees(52.68, 13.59);
    S2PointRegion region(latlng.ToPoint());
    S2Polygon polygon;

    std::vector<S2CellId> cover; // intervalSet
    // get2dsphereCovering
    S2RegionCoverer coverer;
    coverer.set_min_level(0);
    coverer.set_max_level(23);
    coverer.set_max_cells(20);

    coverer.GetCovering(region, &cover);

    CHECK(cover.size() == 1);
    CHECK(cover.front().is_valid());

    // S2CellIdsToIntervalsWithParents(
    S2CellId interval = S2CellId::FromLatLng(latlng);
    cover.push_back(interval);
}

#endif
