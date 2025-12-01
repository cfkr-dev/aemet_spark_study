"use client"

import * as React from "react"
import Link from "next/link"

import { useIsMobile } from "@/hooks/use-mobile"
import {
    NavigationMenu,
    NavigationMenuContent,
    NavigationMenuItem,
    NavigationMenuLink,
    NavigationMenuList,
    NavigationMenuTrigger
} from "@/components/ui/navigation-menu"


export function NavigationCustomMenu() {
    const isMobile = useIsMobile()

    const handleLinkClick = (e: React.MouseEvent<HTMLAnchorElement>, id: string) => {
        e.preventDefault();

        const container = document.getElementById(id);
        if (!container) return;

        const event = new CustomEvent("open-accordion", { bubbles: true });
        container.dispatchEvent(event); // se dispara directamente sobre el acordeón
    };

    return (
        <NavigationMenu viewport={isMobile}>
            <NavigationMenuList className="flex-wrap space-x-5">
                <NavigationMenuItem className="hidden md:block">
                    <NavigationMenuTrigger className="text-lg">Meteorological station studies</NavigationMenuTrigger>
                    <NavigationMenuContent>
                        <ul className="grid w-[300px] gap-4">
                            <li>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "stations-count_by_state_2024")}
                                    >
                                        <div className="font-medium">Stations count by state</div>
                                        <div className="text-muted-foreground">
                                            Number of stations per state as of 2024.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "stations-count_by_altitude_2024")}
                                    >
                                        <div className="font-medium">Station count by altitude</div>
                                        <div className="text-muted-foreground">
                                            Number of stations distributed by altitude as of 2024.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "stations-count_evol")}
                                    >
                                        <div className="font-medium">Station count evolution</div>
                                        <div className="text-muted-foreground">
                                            Evolution of the number of stations from 1973 to 2024.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                            </li>
                        </ul>
                    </NavigationMenuContent>
                </NavigationMenuItem>
                <NavigationMenuItem className="hidden md:block">
                    <NavigationMenuTrigger className="text-lg">Climograph studies</NavigationMenuTrigger>
                    <NavigationMenuContent>
                        <ul className="grid w-[300px] gap-4">
                            <li>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "climographs-arid")}
                                    >
                                        <div className="font-medium">Arid</div>
                                        <div className="text-muted-foreground">
                                            Arid climate climographs.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "climographs-warm")}
                                    >
                                        <div className="font-medium">Warm</div>
                                        <div className="text-muted-foreground">
                                            Warm climate climographs.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "climographs-cold")}
                                    >
                                        <div className="font-medium">Cold</div>
                                        <div className="text-muted-foreground">
                                            Cold climate climographs.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                            </li>
                        </ul>
                    </NavigationMenuContent>
                </NavigationMenuItem>
                <NavigationMenuItem className="hidden md:block">
                    <NavigationMenuTrigger className="text-lg">Meteorological parameter studies</NavigationMenuTrigger>
                    <NavigationMenuContent>
                        <ul className="grid w-[300px] gap-4">
                            <li>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "single_param_studies-temp")}
                                    >
                                        <div className="font-medium">Temperature</div>
                                        <div className="text-muted-foreground">
                                            Temperature studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "single_param_studies-prec")}
                                    >
                                        <div className="font-medium">Precipitation</div>
                                        <div className="text-muted-foreground">
                                            Precipitation studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "single_param_studies-wind_vel")}
                                    >
                                        <div className="font-medium">Wind velocity</div>
                                        <div className="text-muted-foreground">
                                            Wind velocity studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "single_param_studies-press")}
                                    >
                                        <div className="font-medium">Atmospheric pressure</div>
                                        <div className="text-muted-foreground">
                                            Atmospheric pressure studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "single_param_studies-sun_rad")}
                                    >
                                        <div className="font-medium">Sun radiation</div>
                                        <div className="text-muted-foreground">
                                            Sun radiation studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "single_param_studies-rel_hum")}
                                    >
                                        <div className="font-medium">Relative humidity</div>
                                        <div className="text-muted-foreground">
                                            Relative humidity studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                            </li>
                        </ul>
                    </NavigationMenuContent>
                </NavigationMenuItem>
                <NavigationMenuItem className="hidden md:block">
                    <NavigationMenuTrigger className="text-lg">Meteorological interesting studies</NavigationMenuTrigger>
                    <NavigationMenuContent>
                        <ul className="grid w-[300px] gap-4">
                            <li>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "interesting_studies-prec_and_press_evol")}
                                    >
                                        <div className="font-medium">Precipitation and atmospheric pressure</div>
                                        <div className="text-muted-foreground">
                                            Correlation between precipitation and atmospheric pressure studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                                <NavigationMenuLink asChild>
                                    <Link
                                        href=""
                                        onClick={(e) => handleLinkClick(e, "interesting_studies-top_10_states")}
                                    >
                                        <div className="font-medium">Top 10 better states for special conditions</div>
                                        <div className="text-muted-foreground">
                                            Better states for special conditions studies.
                                        </div>
                                    </Link>
                                </NavigationMenuLink>
                            </li>
                        </ul>
                    </NavigationMenuContent>
                </NavigationMenuItem>
            </NavigationMenuList>
        </NavigationMenu>
    )
}
