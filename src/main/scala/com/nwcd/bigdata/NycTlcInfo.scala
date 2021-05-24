package com.nwcd.bigdata

import java.util.Date

case class NycTlcInfo(
                       var vendorID:String,
                       var puDT:Date,
                       var doDT:Date,
                       var diffDT:Int,
                       var distance:Float,
                       var avgSpeed:Float,
                       var puLID:String,
                       var doLID:String,
                       var ts:Long) {
}