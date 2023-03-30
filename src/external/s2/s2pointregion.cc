// Copyright 2005 Google Inc. All Rights Reserved.

#include "s2pointregion.h"
#include "base/logging.h"
#include "s2cap.h"
#include "s2cell.h"
#include "s2latlngrect.h"

static const unsigned char kCurrentEncodingVersionNumber = 1;

S2PointRegion::~S2PointRegion() {
}

S2PointRegion* S2PointRegion::Clone() const {
  return new S2PointRegion(point_);
}

S2Cap S2PointRegion::GetCapBound() const {
  return S2Cap::FromAxisHeight(point_, 0);
}

S2LatLngRect S2PointRegion::GetRectBound() const {
  S2LatLng ll(point_);
  return S2LatLngRect(ll, ll);
}

bool S2PointRegion::MayIntersect(S2Cell const& cell) const {
  return cell.Contains(point_);
}
