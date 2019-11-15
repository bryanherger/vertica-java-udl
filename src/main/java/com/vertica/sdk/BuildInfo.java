/* 
 * Copyright (c) 2007 EntIT Software LLC, a Micro Focus company
 *
 * Description: Support code for UDx subsystem
 *
 * Create Date: Thu May 16 21:37:39 2019
 */
/* Build-time identification for VerticaSDKJava */
package com.vertica.sdk;
public class BuildInfo {
    public static final String VERTICA_BUILD_ID_Brand_Name       = "Vertica Analytic Database";
    public static final String VERTICA_BUILD_ID_Brand_Version    = "v9.2.1-1";
    public static final String VERTICA_BUILD_ID_SDK_Version      = "9.2.1";
    public static final String VERTICA_BUILD_ID_Codename         = "Grader";
    public static final String VERTICA_BUILD_ID_Date             = "Thu May 16 21:37:39 2019";
    public static final String VERTICA_BUILD_ID_Machine          = "re-docker2";
    public static final String VERTICA_BUILD_ID_Branch           = "tag";
    public static final String VERTICA_BUILD_ID_Revision         = "releases/VER_9_2_RELEASE_BUILD_1_1_20190516";
    public static final String VERTICA_BUILD_ID_Checksum         = "a2af2c712d0cdd739699318fb1fabd91";

    public static VerticaBuildInfo get_vertica_build_info() {
        VerticaBuildInfo vbi = new VerticaBuildInfo();
        vbi.brand_name      = BuildInfo.VERTICA_BUILD_ID_Brand_Name;
        vbi.brand_version   = BuildInfo.VERTICA_BUILD_ID_Brand_Version;
	    vbi.sdk_version     = BuildInfo.VERTICA_BUILD_ID_SDK_Version;
        vbi.codename        = BuildInfo.VERTICA_BUILD_ID_Codename;
        vbi.build_date      = BuildInfo.VERTICA_BUILD_ID_Date;
        vbi.build_machine   = BuildInfo.VERTICA_BUILD_ID_Machine;
        vbi.branch          = BuildInfo.VERTICA_BUILD_ID_Branch;
        vbi.revision        = BuildInfo.VERTICA_BUILD_ID_Revision;
        vbi.checksum        = BuildInfo.VERTICA_BUILD_ID_Checksum;
        return vbi;
    }
}
/* end of this file */
