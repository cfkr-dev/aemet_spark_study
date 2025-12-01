"use client";

import { useEffect, useState } from "react";
import { Accordion, AccordionItem, AccordionTrigger, AccordionContent } from "@/components/ui/accordion";
import { Skeleton } from "@/components/ui/skeleton";
import IframeViewer from "@/components/custom-ui/iframe-viewer";

interface AccordionCustomOpenIframeProps {
    title: string;
    subtitle?: string;
    src: string;
    className?: string;
    id: string;
}

export default function AccordionCustomOpenIframe({ title, subtitle, src, className, id }: AccordionCustomOpenIframeProps) {
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const container = document.getElementById(id);
        if (!container) return;

        const scrollToAccordion = () => {
            const header = document.querySelector("header");
            if (header) {
                const headerHeight = header.getBoundingClientRect().height;
                const margin = 8;
                const elementPosition = container.getBoundingClientRect().top + window.scrollY;
                const offsetPosition = elementPosition - headerHeight - margin;

                window.scrollTo({
                    top: offsetPosition,
                    behavior: "smooth",
                });
            }
        };

        // Animación de apertura
        const handleAnimationEnd = (e: AnimationEvent) => {
            if (e.animationName === "accordion-down") {
                scrollToAccordion();
            }
        };
        container.addEventListener("animationend", handleAnimationEnd);

        // Listener para el evento disparado directamente en este elemento
        const handleOpenEvent = () => {
            const trigger = document.getElementById(`trigger-${id}`);
            if (!trigger) return;

            const isOpen = trigger.getAttribute("aria-expanded") === "true";

            if (!isOpen) {
                trigger.click(); // scroll se hará en animationend
            } else {
                scrollToAccordion(); // ya abierto → scroll inmediato
            }
        };

        container.addEventListener("open-accordion", handleOpenEvent);

        return () => {
            container.removeEventListener("animationend", handleAnimationEnd);
            container.removeEventListener("open-accordion", handleOpenEvent);
        };
    }, [id]);


    const handleOpenChange = (value: string | null) => {
        if (value === "item-1") {
            setLoading(true);
        }
    };

    return (
        <Accordion
            type="single"
            collapsible
            className={`mt-4 rounded ${className || ""}`}
            onValueChange={handleOpenChange}
            id={id}
        >
            <AccordionItem
                value="item-1"
                className="transition-shadow duration-300 shadow-md bg-gray-50 rounded"
            >
                <AccordionTrigger
                    id={`trigger-${id}`}
                    className="flex justify-between items-center w-full cursor-pointer px-4 py-3 rounded hover:bg-gray-100 transition-colors"
                >
                    <h3 className="text-lg font-semibold">{title}</h3>
                </AccordionTrigger>

                <AccordionContent className="mt-2 px-4 pb-4">
                    {subtitle && <p className="text-sm text-gray-500 my-2">{subtitle}</p>}
                    <div className="w-full h-[75vh]">
                        {loading && <Skeleton className="w-full h-full rounded bg-gray-200" />}
                        <IframeViewer
                            src={src}
                            className="w-full h-full rounded shadow"
                            onLoad={() => setLoading(false)}
                        />
                    </div>
                </AccordionContent>
            </AccordionItem>
        </Accordion>
    );
}
