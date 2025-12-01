"use client";

import * as React from "react";
import { useState } from "react";
import { Check, ChevronsUpDown, X } from "lucide-react";

import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";

export interface Option { value: string; label: string }
interface SelectSearchCustomProps {options: Option[], childrenFn: ({value, label}: Option) => React.ReactNode}

export default function SelectSearchCustom({options, childrenFn}: SelectSearchCustomProps) {
    const [value, setValue] = useState<string | null>(null);
    const [open, setOpen] = useState(false);

    const selectedOption = options.find((o) => o.value === value);

    const handleSelect = (val: string) => {
        if (val === value) setValue(null);
        else setValue(val);
        setOpen(false);
    };

    return (
        <div className="p-2 w-full">
            <div className="relative w-1/5">
                <Popover open={open} onOpenChange={setOpen}>
                    <PopoverTrigger asChild>
                        <Button
                            variant="outline"
                            role="combobox"
                            aria-expanded={open}
                            className="w-full justify-between pr-10 relative"
                        >
                            <span className="truncate">{selectedOption ? selectedOption.label : "Selecciona una opción"}</span>

                            <div className="absolute right-3 flex items-center space-x-1">
                                {value && (
                                    <div
                                        className="opacity-70 hover:opacity-100 cursor-pointer"
                                        onClick={(e) => {
                                            e.preventDefault();
                                            e.stopPropagation();
                                            setValue(null);
                                        }}
                                    >
                                        <X className="h-4 w-4 text-gray-500" />
                                    </div>
                                )}

                                <ChevronsUpDown className="h-4 w-4 opacity-50" />
                            </div>
                        </Button>
                    </PopoverTrigger>

                    <PopoverContent className="w-[280px] p-0" side="bottom" sideOffset={6} align="start">
                        <Command>
                            <CommandInput placeholder="Buscar..." />
                            <CommandEmpty>No hay resultados.</CommandEmpty>
                            <CommandList>
                                <CommandGroup>
                                    {options.map((opt) => (
                                        <CommandItem key={opt.value} value={opt.value} onSelect={() => handleSelect(opt.value)}>
                                            <Check className={cn("mr-2 h-4 w-4", value === opt.value ? "opacity-100" : "opacity-0")} />
                                            {opt.label}
                                        </CommandItem>
                                    ))}
                                </CommandGroup>
                            </CommandList>
                        </Command>
                    </PopoverContent>
                </Popover>
            </div>

            {value && selectedOption && childrenFn(selectedOption)}
        </div>
    );
}
