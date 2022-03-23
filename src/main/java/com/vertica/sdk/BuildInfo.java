/*
 * Copyright (c) 2007 EntIT Software LLC, a Micro Focus company
 *
 * Description: Support code for UDx subsystem
 *
 * Create Date: Thu Feb 10 22:29:43 2022
 */
/* Build-time identification for VerticaSDKJava */
package com.vertica.sdk;
public class BuildInfo {
    public static final String VERTICA_BUILD_ID_Brand_Name         = "Vertica Analytic Database";
    public static final String VERTICA_BUILD_ID_Brand_Version      = "v11.1.0-0";
    public static final String VERTICA_BUILD_ID_SDK_Version        = "11.0.1";
    public static final int    VERTICA_BUILD_ID_SDK_Version_Major  =  11;
    public static final int    VERTICA_BUILD_ID_SDK_Version_Minor  =  0;
    public static final int    VERTICA_BUILD_ID_SDK_Version_SP     =  1;
    public static final int    VERTICA_BUILD_ID_SDK_Version_Hotfix =  0;
    public static final long   VERTICA_BUILD_ID_SDK_Version_Int    =  VERTICA_BUILD_ID_SDK_Version_as_int(VERTICA_BUILD_ID_SDK_Version_Major, VERTICA_BUILD_ID_SDK_Version_Minor, VERTICA_BUILD_ID_SDK_Version_SP, VERTICA_BUILD_ID_SDK_Version_Hotfix);
    public static final String VERTICA_BUILD_ID_Codename           = "Jackhammer";
    public static final String VERTICA_BUILD_ID_Date               = "Thu Feb 10 22:29:43 2022";
    public static final String VERTICA_BUILD_ID_Machine            = "re-docker4";
    public static final String VERTICA_BUILD_ID_Branch             = "releases/VER_11_1_RELEASE_BUILD_0_0_20220210";
    public static final String VERTICA_BUILD_ID_Revision           = "669fd97287b9c05ae8b69656c04c16cffa268024";
    public static final String VERTICA_BUILD_ID_Checksum           = "9b6969ccbfdb6702f98f2b0ac67359f9";

    public static long VERTICA_BUILD_ID_SDK_Version_as_int(int major, int minor, int sp, int hotfix) {
        return ( (major & 0xFF) << 24 | (minor & 0xFF) << 16 | (sp & 0xFF) << 8 | (hotfix & 0xFF) );
    }
    public static Boolean VERTICA_BUILD_ID_SDK_Version_at_least(int major, int minor, int sp, int hotfix) {
        return (VERTICA_BUILD_ID_SDK_Version_as_int(major,minor,sp,hotfix) >= VERTICA_BUILD_ID_SDK_Version_Int);
    }
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
