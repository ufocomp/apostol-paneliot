/*++

Library name:

  apostol-core

Module Name:

  Processes.hpp

Notices:

  Application others processes

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_PROCESSES_HPP
#define APOSTOL_PROCESSES_HPP
//----------------------------------------------------------------------------------------------------------------------

#include "Header.hpp"
//----------------------------------------------------------------------------------------------------------------------

#include "StreamServer/StreamServer.hpp"
//----------------------------------------------------------------------------------------------------------------------

static inline void CreateProcesses(CCustomProcess *AParent, CApplication *AApplication) {
    CStreamServer::CreateProcess(AParent, AApplication);
}

#endif //APOSTOL_PROCESSES_HPP
