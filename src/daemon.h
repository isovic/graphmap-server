/*
 * daemon.h
 *
 *  Created on: Aug 30, 2014
 *      Author: ivan
 */

#ifndef DAEMON_H_
#define DAEMON_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <sstream>
#include <map>
#include <list>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/signalfd.h>
#include <sys/inotify.h>
#include <signal.h>
#include <limits.h>
#include <fcntl.h>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <iostream>
#include <fstream>
#include <time.h>
#include <vector>
#include <dirent.h>
#include <deque>
#include "graphmap/graphmap.h"

#include "semaphore.h"

#define EVENT_SIZE          ( sizeof (struct inotify_event) )
#define EVENT_BUF_LEN       ( 1024 * ( EVENT_SIZE + NAME_MAX + 1) )
#define WATCH_FLAGS         ( IN_CREATE | IN_MODIFY | IN_CLOSE | IN_MOVE )

void SigCallback(int sig);

typedef std::map<std::string, int> FileMonitorType;



class Daemon {
 public:
  ~Daemon();
  static Daemon& GetInstance();

  void Run(std::string watch_folder, std::string output_folder, std::string task_extension, bool is_dry_run, GraphMap *graphmap, const ProgramParameters &parameters);
  bool is_run() const;
  void set_run(bool run);

  friend void SigCallback(int sig);

 private:
  Daemon();
  Daemon(Daemon const&);              // Don't Implement
  void operator=(Daemon const&);      // Don't implement

  void RunNotifier_();
  void RunJobs_();
  void ProcessSingleJob_(std::string &file_name);
  bool StringEndsWith_(std::string const &full_string, std::string const &ending);
  void ParseTaskFile_(std::string task_file_path);
  bool GetFileList_(std::string folder, std::vector<std::string> &ret_files);
  std::string TrimString_(std::string &input_string);
  void PopulateQueueFromFolder_(std::string folder_path);
  std::string GetUTCTime_();

  std::string watch_folder_;
  std::string output_folder_;
  std::string processing_folder_;
  std::string task_extension_;
  bool is_dry_run_;

  std::deque<std::string> files_to_process_;
  bool run_;

  std::unique_ptr<Semaphore> queue_sem_;
  std::unique_ptr<Semaphore> active_sem_;
  bool terminate_;

  GraphMap *graphmap_;
  ProgramParameters parameters_;
};

#endif /* DAEMON_H_ */
